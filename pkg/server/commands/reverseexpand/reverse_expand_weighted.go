package reverseexpand

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func (c *ReverseExpandQuery) loopOverEdgesUsingWeigtedGraph(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
	wg *weightedGraph.WeightedAuthorizationModelGraph,
	sourceUserType, sourceUserObj string,
) error {
	targetTypeRel := req.weightedEdgeTypeRel
	// This is true on the first call of reverse expand
	if targetTypeRel == "" {
		targetObjRef := typesystem.DirectRelationReference(req.ObjectType, req.Relation)

		targetTypeRel = targetObjRef.GetType() + "#" + targetObjRef.GetRelation()
	}

	edges, needsCheck, err := c.getEdgesFromWeightedGraph(
		wg,
		targetTypeRel,
		sourceUserType,
		intersectionOrExclusionInPreviousEdges,
	)

	if err != nil {
		return err
	}

	// if it's a userset and there no edges, just readTuplesAndExecuteWeighted with the type#rel
	// and return
	if edges == nil || len(edges) == 0 {
		return c.readTuplesAndExecuteWeighted(ctx, req, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
	}

	return c.loopOverWeightedEdges(
		ctx,
		edges,
		needsCheck,
		req,
		resolutionMetadata,
		resultChan,
		sourceUserObj,
	)
}

// GetEdgesFromWeightedGraph returns a list of edges, boolean indicating whether Check is needed, and an error.
func (c *ReverseExpandQuery) getEdgesFromWeightedGraph(
	wg *weightedGraph.WeightedAuthorizationModelGraph,
	targetTypeRelation string,
	sourceType string,
	needsCheck bool,
) ([]*weightedGraph.WeightedAuthorizationModelEdge, bool, error) {
	if wg == nil {
		// this should never happen
		return nil, false, errors.New("weighted graph is nil")
	}

	currentNode, ok := wg.GetNodeByID(targetTypeRelation)
	if !ok {
		// This should never happen
		return nil, false, errors.New("currentNode is nil")
	}

	// This means we cannot reach the source type requested.
	// e.g. there is no path from 'document' to 'user'
	if _, ok = currentNode.GetWeight(sourceType); !ok {
		if edges, ok := wg.GetEdgesFromNode(currentNode); ok {
			return edges, needsCheck, nil
		}
		return nil, false, nil
	}

	edges, _ := wg.GetEdgesFromNode(currentNode)

	// TODO: this _shouldn't_ be reachable but will be dealt with in a follow up PR
	// This would mean that we dispatched from a direct edge, which doesn't make sense
	if len(edges) == 0 {
		return nil, false, errors.New("no outgoing edges")
	}

	if currentNode.GetNodeType() == weightedGraph.OperatorNode {
		switch currentNode.GetLabel() {
		case weightedGraph.ExclusionOperator: // e.g. rel1: [user, other] BUT NOT b
			butNotEdge := edges[len(edges)-1] // this is the edge to 'b'
			_, canReachSource := butNotEdge.GetWeight(sourceType)

			// if the 'b' in BUT NOT b has a weight for the terminal type we're seeking
			// we need to run check at the end
			if canReachSource {
				needsCheck = true
			}

			// prune off the "BUT NOT b" portion of these edges and keep going
			// the right-most edge is ALWAYS the "BUT NOT", so trim the last element
			edges = edges[:len(edges)-1]
		case weightedGraph.IntersectionOperator:
			// For AND relations, mark as "needs check" and just pick the lowest weight edge
			needsCheck = true

			lowestWeightEdge := reduce(edges, nil, cheapestEdgeTo(sourceType))

			// return only the lowest weight edge
			edges = []*weightedGraph.WeightedAuthorizationModelEdge{lowestWeightEdge}
		}
	}

	// Filter to only return edges which have a path to the sourceType
	relevantEdges := filter(edges, hasPathTo(sourceType))

	return relevantEdges, needsCheck, nil
}

func (c *ReverseExpandQuery) loopOverWeightedEdges(
	ctx context.Context,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	needsCheck bool,
	req *ReverseExpandRequest,
	resolutionMetadata *ResolutionMetadata,
	resultChan chan<- *ReverseExpandResult,
	sourceUserObj string,
) error {
	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	var errs error

	for _, edge := range edges {
		// TODO: i think the getEdgesFromWeightedGraph func handles this for us, shouldn't need this
		// intersectionOrExclusionInPreviousEdges := intersectionOrExclusionInPreviousEdges || innerLoopEdge.TargetReferenceInvolvesIntersectionOrExclusion
		//innerLoopEdge := edge
		r := &ReverseExpandRequest{
			Consistency:      req.Consistency,
			Context:          req.Context,
			ContextualTuples: req.ContextualTuples,
			ObjectType:       req.ObjectType,
			Relation:         req.Relation,
			StoreID:          req.StoreID,
			User:             req.User,

			weightedEdge:        edge,
			weightedEdgeTypeRel: edge.GetTo().GetUniqueLabel(),
			edge:                req.edge,
		}
		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			fmt.Printf("User going into direct query: %+v\n", r.User)
			pool.Go(func(ctx context.Context) error {
				return c.reverseExpandDirectWeighted(ctx, r, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.ComputedEdge:
			//fmt.Printf("JUSTIN Computed edge from %s to %s\n", edge.GetFrom().GetUniqueLabel(), edge.GetTo().GetUniqueLabel())
			// follow the computed_userset edge, no new goroutine needed since it's not I/O intensive
			to := edge.GetTo().GetUniqueLabel()

			// turn "document#viewer" into "viewer"
			rel := getRelationFromLabel(to)
			r.User = &UserRefObjectRelation{
				ObjectRelation: &openfgav1.ObjectRelation{
					Object:   sourceUserObj,
					Relation: rel,
				},
			}
			fmt.Printf("User going into computed dispatch: %+v\n", r.User)
			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.TTUEdge:
			//fmt.Printf("JUSTIN TTU EDGE")
			pool.Go(func(ctx context.Context) error {
				return c.reverseExpandTupleToUsersetWeighted(ctx, r, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.RewriteEdge:
			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		default:
			fmt.Printf("Unknown edge type %d\n", edge.GetEdgeType())
			panic("unsupported edge type")
		}
	}

	return errors.Join(errs, pool.Wait())
	// Can we lift this to the caller? I don't want to have to pass in a span also this method signature is big
	// if errs != nil {
	//	telemetry.TraceError(span, errs)
	//	return errs
	//}
}

func (c *ReverseExpandQuery) reverseExpandDirectWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandDirect", trace.WithAttributes(
		// attribute.String("edge", req.edge.String()),
		attribute.String("source.user", req.User.String()),
	))
	var err error
	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
		}
		span.End()
	}()

	err = c.readTuplesAndExecuteWeighted(ctx, req, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
	return err
}

func (c *ReverseExpandQuery) reverseExpandTupleToUsersetWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandTupleToUsersetWeighted", trace.WithAttributes(
		attribute.String("edge.from", req.weightedEdge.GetFrom().GetUniqueLabel()),
		attribute.String("edge.to", req.weightedEdge.GetFrom().GetUniqueLabel()),
		attribute.String("source.user", req.User.String()),
	))
	var err error
	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
		}
		span.End()
	}()

	err = c.readTuplesAndExecuteWeighted(ctx, req, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
	return err
}

func (c *ReverseExpandQuery) readTuplesAndExecuteWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	ctx, span := tracer.Start(ctx, "readTuplesAndExecuteWeighted")
	defer span.End()

	var userFilter []*openfgav1.ObjectRelation
	var relationFilter string

	userFilter, relationFilter, err := c.buildQueryFiltersWeighted(ctx, req)
	if err != nil {
		return err
	}

	// find all tuples of the form req.edge.TargetReference.Type:...#relationFilter@userFilter
	iter, err := c.datastore.ReadStartingWithUser(ctx, req.StoreID, storage.ReadStartingWithUserFilter{
		ObjectType: getTypeFromLabel(req.weightedEdge.GetFrom().GetLabel()), // e.g. directs-employee
		Relation:   relationFilter,                                          // other-rel
		UserFilter: userFilter,                                              // .Object = employee#alg_combined_1
	}, storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.Consistency,
		},
	})
	if err != nil {
		return err
	}
	fmt.Println("--------WEIGHTED-----------------")
	fmt.Printf(" UserFilter: %s\n, RelationFilter %s\n", userFilter, relationFilter)

	// filter out invalid tuples yielded by the database iterator
	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(c.typesystem),
	)
	defer filteredIter.Stop()

	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	var errs error

LoopOnIterator:
	for {
		tk, err := filteredIter.Next(ctx)
		fmt.Printf("JUSTIN TUPLE KEY: %s\n", tk.String())
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			errs = errors.Join(errs, err)
			break LoopOnIterator
		}

		condEvalResult, err := eval.EvaluateTupleCondition(ctx, tk, c.typesystem, req.Context)
		if err != nil {
			fmt.Printf("JUSTIN TK Condition failed: %s\n", tk.String())
			errs = errors.Join(errs, err)
			continue
		}

		if !condEvalResult.ConditionMet {
			if len(condEvalResult.MissingParameters) > 0 {
				errs = errors.Join(errs, condition.NewEvaluationError(
					tk.GetCondition().GetName(),
					fmt.Errorf("tuple '%s' is missing context parameters '%v'",
						tuple.TupleKeyToString(tk),
						condEvalResult.MissingParameters),
				))
			}

			continue
		}

		foundObject := tk.GetObject()
		var newRelation string
		fmt.Printf("JUSTIN found object: %s\n", tk.GetObject())

		switch req.weightedEdge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			// for direct edge I think we can just emit this and be done
			// need the "needs check" bit
			err := c.trySendCandidate(ctx, intersectionOrExclusionInPreviousEdges, foundObject, resultChan)
			errs = errors.Join(errs, err)

			continue

		case weightedGraph.TTUEdge:
			newRelation = req.weightedEdge.GetTo().GetLabel() // TODO : validate this?
		default:
			panic("unsupported edge type")
		}

		// Do we still need this additional dispatch?
		pool.Go(func(ctx context.Context) error {
			return c.dispatch(ctx, &ReverseExpandRequest{
				StoreID:    req.StoreID,
				ObjectType: req.ObjectType,
				Relation:   req.Relation,
				User: &UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   foundObject,
						Relation: newRelation,
					},
					Condition: tk.GetCondition(),
				},
				ContextualTuples: req.ContextualTuples,
				Context:          req.Context,
				edge:             req.edge,
				Consistency:      req.Consistency,
				// TODO : verify this
				weightedEdge:        req.weightedEdge,
				weightedEdgeTypeRel: req.weightedEdgeTypeRel,
			}, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
		})
	}

	errs = errors.Join(errs, pool.Wait())
	if errs != nil {
		telemetry.TraceError(span, errs)
		return errs
	}

	return nil
}

func (c *ReverseExpandQuery) buildQueryFiltersWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
) ([]*openfgav1.ObjectRelation, string, error) {
	var userFilter []*openfgav1.ObjectRelation
	var relationFilter string

	// Should this actually be looking at the node we're heading towards?
	switch req.weightedEdge.GetEdgeType() {
	case weightedGraph.DirectEdge:
		// the .From() for a direct edge will have a type#rel e.g. directs-employee#other_rel
		fromLabel := req.weightedEdge.GetFrom().GetLabel()
		relationFilter = getRelationFromLabel(fromLabel) // directs-employee#other_rel -> other_rel

		toNode := req.weightedEdge.GetTo()

		//targetUserObjectType := toNode.GetLabel() // "employee"
		fmt.Printf("JUSTIN buildQueryFilters To node type: %d, label: %s\n", toNode.GetNodeType(), toNode.GetLabel())

		// e.g. 'user:*'
		if toNode.GetNodeType() == weightedGraph.SpecificTypeWildcard {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: toNode.GetLabel(), // e.g. "employee:*"
			})
		}

		// Should we just be using nodes?
		if toNode.GetNodeType() == weightedGraph.SpecificType {
		}

		// e.g. 'user:bob'
		if val, ok := req.User.(*UserRefObject); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: tuple.BuildObject(val.Object.GetType(), val.Object.GetId()),
			})
		}

		// e.g. 'group:eng#member'
		// so is it if the TO node is direct to a userset?
		// which would be a DirectEdge TO node with type weightedGraph.SpecificTypeAndRelation
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			if toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation {
				userFilter = append(userFilter, val.ObjectRelation)
			} else if toNode.GetNodeType() == weightedGraph.SpecificType {
				userFilter = append(userFilter, &openfgav1.ObjectRelation{
					Object: val.ObjectRelation.GetObject(),
				})
			}
			//fmt.Printf("JUSTIN adding a relation piece: %s - node type: %d\n", val.ObjectRelation, toNode.GetNodeType())
		}
	case weightedGraph.TTUEdge:
		relationFilter = req.edge.TuplesetRelation
		// a TTU edge can only have a userset as a source node
		// e.g. 'group:eng#member'
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: val.ObjectRelation.GetObject(),
			})
			fmt.Printf("TTU EDGE GetObject(): %s", val.ObjectRelation.GetObject())
		} else {
			panic("unexpected source for reverse expansion of tuple to userset")
		}
	// TODO: are there any other cases?
	default:
		panic("unsupported edge type")
	}

	return userFilter, relationFilter, nil
}

func hasPathTo(dest string) func(*weightedGraph.WeightedAuthorizationModelEdge) bool {
	return func(edge *weightedGraph.WeightedAuthorizationModelEdge) bool {
		_, ok := edge.GetWeight(dest)
		return ok
	}
}

func cheapestEdgeTo(dst string) func(*weightedGraph.WeightedAuthorizationModelEdge, *weightedGraph.WeightedAuthorizationModelEdge) *weightedGraph.WeightedAuthorizationModelEdge {
	return func(lowest, current *weightedGraph.WeightedAuthorizationModelEdge) *weightedGraph.WeightedAuthorizationModelEdge {
		if lowest == nil {
			return current
		}

		a, ok := lowest.GetWeight(dst)
		if !ok {
			return current
		}

		b, ok := current.GetWeight(dst)
		if !ok {
			return lowest
		}

		if b < a {
			return current
		}
		return lowest
	}
}

func reduce[S ~[]E, E any, A any](s S, initializer A, f func(A, E) A) A {
	i := initializer
	for _, item := range s {
		i = f(i, item)
	}
	return i
}

func filter[S ~[]E, E any](s S, f func(E) bool) []E {
	var filteredItems []E
	for _, item := range s {
		if f(item) {
			filteredItems = append(filteredItems, item)
		}
	}
	return filteredItems
}

// expects a "type#rel".
func getTypeFromLabel(label string) string {
	userObject, _ := tuple.SplitObjectRelation(label)
	return userObject
}

// expects a "type#rel".
func getRelationFromLabel(label string) string {
	_, rel := tuple.SplitObjectRelation(label)
	return rel
}
