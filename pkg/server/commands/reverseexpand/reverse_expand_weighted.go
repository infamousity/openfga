package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"sync"
)

// relationStack is a stack of type#rel strings we build while traversing the graph to locate leaf nodes
type relationStack []string

func (r *relationStack) Push(value string) {
	*r = append(*r, value)
}

func (r *relationStack) Pop() string {
	element := (*r)[len(*r)-1]
	*r = (*r)[0 : len(*r)-1]
	return element
}

func (r *relationStack) Copy() []string {
	dst := make(relationStack, len(*r)) // Create a new slice with the same length
	copy(dst, *r)
	return dst
}

func (r *relationStack) Len() int {
	return len(*r)
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
	pool := concurrency.NewPool(ctx, 1)

	var errs error

	for _, edge := range edges {
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
			stack:               req.stack.Copy(),
		}
		fmt.Printf("Justin Edge: %s --> %s Stack: %s\n",
			edge.GetFrom().GetUniqueLabel(),
			edge.GetTo().GetUniqueLabel(),
			r.stack.Copy(),
		)
		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			fmt.Printf("Justin Direct Edge: \n\t%s\n\t%s\n\t%s\n",
				edge.GetFrom().GetUniqueLabel(),
				edge.GetTo().GetUniqueLabel(),
				r.stack.Copy(),
			)
			// Now kick off queries for tuples based on the stack of relations we have built to get to this leaf
			pool.Go(func(ctx context.Context) error {
				return c.queryForTuples(
					ctx,
					r,
					needsCheck,
					resultChan, // can we put this on the ReverseExpandQuery itself?
				)
			})
		case weightedGraph.ComputedEdge:
			// TODO: removed logic in here that transformed the usersets to trigger bail out case
			// and prevent infinite loops. need to rebuild that loop prevention with weighted graph data.
			if edge.GetTo().GetNodeType() != weightedGraph.OperatorNode {
				_ = r.stack.Pop()
				r.stack.Push(edge.GetTo().GetUniqueLabel())
			}

			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.TTUEdge: // This is not handled yet
			fmt.Printf("Justin TTU Edge: \n\t%s\n\t%s\n\t%s\n",
				edge.GetFrom().GetUniqueLabel(),
				edge.GetTo().GetUniqueLabel(),
				r.stack.Copy(),
			)
			if edge.GetTo().GetNodeType() != weightedGraph.OperatorNode {
				// Replace the existing type#rel on the stack with the TTU relation
				_ = r.stack.Pop()
				r.stack.Push(edge.GetTuplesetRelation())
				r.stack.Push(edge.GetTo().GetUniqueLabel())
			}
			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
			//pool.Go(func(ctx context.Context) error {
			//	return c.queryForTuples(
			//		ctx,
			//		r,
			//		needsCheck,
			//		resultChan, // can we put this on the ReverseExpandQuery itself?
			//	)
			//})
		case weightedGraph.RewriteEdge:
			// bc operator nodes are not real types
			if edge.GetTo().GetNodeType() != weightedGraph.OperatorNode {
				_ = r.stack.Pop()
				r.stack.Push(edge.GetTo().GetUniqueLabel())
			}
			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		default:
			panic("unsupported edge type")
		}
	}

	return errors.Join(errs, pool.Wait())
}

func (c *ReverseExpandQuery) queryForTuples(
	ctx context.Context,
	req *ReverseExpandRequest,
	needsCheck bool,
	resultChan chan<- *ReverseExpandResult,
) error {
	// This method should be recursive, and handle all tuple querying and also emitting
	// to channel when it hits the base case.
	// to make this more usable it might be better to directly pass in the needed params, rather than
	// copying the whole ReverseExpandRequest like we do everywhere else
	// TODO: don't forget telemetry

	// Direct edges after TTUs can come in looking like this, with the To of a userset
	//	Justin Direct Edge:
	//	organization#repo_admin // From()
	//	organization#member // This was the To(). So in this case we'd need to NOT pop from the stack and query for
	//	[repo#owner]		// organizations this user is a member of right off the bat, then use the stack

	//if to.GetNodeType() != weightedGraph.SpecificTypeAndRelation {
	//	typeRel = to.GetUniqueLabel()
	//} else {
	//	typeRel = req.stack.Pop()
	//}

	var wg sync.WaitGroup
	errChan := make(chan error, 100) // random value here, needs tuning

	var queryFunc func(context.Context, *ReverseExpandRequest, string)

	queryFunc = func(qCtx context.Context, r *ReverseExpandRequest, objectOverride string) {
		defer wg.Done()
		if qCtx.Err() != nil {
			return
		}

		var typeRel string
		var userFilter []*openfgav1.ObjectRelation
		// this is true on every call Except the first
		if objectOverride != "" {
			typeRel = r.stack.Pop()
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: objectOverride,
			})
		} else {
			var userID string
			var userType string
			// We will always have a UserRefObject here. Queries that come in for pure usersets do not take this code path.
			// e.g. ListObjects(team:fga#member, document, viewer) will not make it here.
			if val, ok := req.User.(*UserRefObject); ok {
				userType = val.GetObjectType()
				userID = val.Object.GetId()
			}

			to := req.weightedEdge.GetTo()

			switch to.GetNodeType() {
			case weightedGraph.SpecificType: // Direct User Reference. To() -> "user"
				typeRel = req.stack.Pop()
				userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: tuple.BuildObject(to.GetUniqueLabel(), userID)})

			case weightedGraph.SpecificTypeWildcard: // Wildcard Referece To() -> "user:*"
				typeRel = req.stack.Pop()
				userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: to.GetUniqueLabel()})

			case weightedGraph.SpecificTypeAndRelation: // Userset, To() -> "group#member"
				typeRel = to.GetUniqueLabel()

				// For this case, we can't build the ObjectRelation with the To() from the edge, because it doesn't
				// Point at an actual type like "user". So we have to use the userType from the original request
				// to determine whether the requested user is a member of this userset
				userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: tuple.BuildObject(userType, userID)})
			}
		}

		objectType, relation := tuple.SplitObjectRelation(typeRel)
		fmt.Printf("JUSTIN querying: \n\tUserfilter: %s, relation: %s, objectType: %s\n",
			userFilter, relation, objectType,
		)
		iter, err := c.datastore.ReadStartingWithUser(ctx, req.StoreID, storage.ReadStartingWithUserFilter{
			ObjectType: objectType,
			Relation:   relation,
			UserFilter: userFilter,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: req.Consistency,
			},
		})

		if err != nil {
			errChan <- err
			return
		}

		// filter out invalid tuples yielded by the database iterator
		filteredIter := storage.NewFilteredTupleKeyIterator(
			storage.NewTupleKeyIteratorFromTupleIterator(iter),
			validation.FilterInvalidTuples(c.typesystem),
		)
		defer filteredIter.Stop()

	LoopOnIterator:
		for {
			tk, err := filteredIter.Next(ctx)
			if err != nil {
				if !errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				errChan <- err
				break LoopOnIterator
			}
			fmt.Printf("JUSTIN TK FOUND: %s\n", tk.String())

			condEvalResult, err := eval.EvaluateTupleCondition(ctx, tk, c.typesystem, req.Context)
			if err != nil {
				errChan <- err
				continue
			}

			if !condEvalResult.ConditionMet {
				if len(condEvalResult.MissingParameters) > 0 {
					errChan <- condition.NewEvaluationError(
						tk.GetCondition().GetName(),
						fmt.Errorf("tuple '%s' is missing context parameters '%v'",
							tuple.TupleKeyToString(tk),
							condEvalResult.MissingParameters),
					)
				}

				continue
			}
			//fmt.Printf("JUSTIN TK passed conditions: %s\n", tk.String())
			if r.stack.Len() == 0 {
				_ = c.trySendCandidate(ctx, needsCheck, tk.GetObject(), resultChan)
				continue
			} else {
				// trigger query for tuples all over again
				fmt.Println("Recursion")
				foundObject := tk.GetObject() // This will be a "type:id" e.g. "document:roadmap"

				newReq := &ReverseExpandRequest{
					StoreID:          r.StoreID,
					Consistency:      r.Consistency,
					Context:          r.Context,
					ContextualTuples: r.ContextualTuples,
					User:             r.User,
					stack:            r.stack.Copy(),
					weightedEdge:     r.weightedEdge, // Inherited but not used by the override path
				}

				wg.Add(1)
				go queryFunc(qCtx, newReq, foundObject)
				// so now we need to query this object against the last stack type#rel
				// e.g. organization:jz#member@user:justin
				// now we need organization:jz

				// could just do a for loop here
				// for ; stack.Len() > 0 && objectFromQueryStillExists {}

				//c.queryForTuples(ctx, req, needsCheck, resultChan)
				// queryForTuples()
				// new object we just found
				// stack with 1 less element
			}
		}
	}

	// Now kick off the querying without an explicit object override for the first call
	wg.Add(1)
	go queryFunc(ctx, req, "")

	wg.Wait()
	close(errChan)
	close(resultChan)
	var errs error
	for err := range errChan {
		errs = errors.Join(errs, err)
	}
	return errs
}

//func queryWhileWeCan(stack relationStack, typeRel string, object string)

// func (c *ReverseExpandQuery) buildFiltersV2(
//
//	req *ReverseExpandRequest,
//
//	) ([]*openfgav1.ObjectRelation, string, string) {
//		var userFilter []*openfgav1.ObjectRelation
//		var relationFilter string
//
// }
/*
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

	userFilter, relationFilter, objectType := c.buildQueryFiltersWeighted(req)

	// TODO: I think we need to determine the object type in the filters function as well, based on edge type
	// TTUS behave differently from others
	fmt.Printf("JUSTIN Weighted\n"+
		"\trelationFilter: %s\n"+
		"\tuserFilter: %s\n"+
		"\tObject type: %s\n"+
		"\tEdgeType: %d\n",
		relationFilter,
		userFilter,
		objectType,
		req.weightedEdge.GetEdgeType(),
	)

	iter, err := c.datastore.ReadStartingWithUser(ctx, req.StoreID, storage.ReadStartingWithUserFilter{
		ObjectType: objectType,     // e.g. directs-employee
		Relation:   relationFilter, // other-rel
		UserFilter: userFilter,
	}, storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.Consistency,
		},
	})
	if err != nil {
		return err
	}

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
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			errs = errors.Join(errs, err)
			break LoopOnIterator
		}

		condEvalResult, err := eval.EvaluateTupleCondition(ctx, tk, c.typesystem, req.Context)
		if err != nil {
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

		switch req.weightedEdge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			err = c.trySendCandidate(ctx, intersectionOrExclusionInPreviousEdges, foundObject, resultChan)
			errs = errors.Join(errs, err)
			continue
		case weightedGraph.TTUEdge:
			// TODO: what's the right behavior here?
			//newRelation = req.weightedEdge.GetTo().GetLabel()

			// so now we need to see if this object has the requisite relation to the parent
			// e.g. we found 'directs:ttu_alg_2', so check if IT has a direct_parent relation to any TTUs
			// what about multiple nested TTUs tho, or a TTU that resolves to a userset
			newRelation = tuple.GetRelation(req.weightedEdge.GetTuplesetRelation())

		default:
			panic("unsupported edge type")
		}

		// TODO after lunch: you need this to be the direct edge from ttus -> direct parent
		// Stick the WG back on the window and find out how to get it
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
				// TODO: what edge do i give it tho
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
	req *ReverseExpandRequest,
) ([]*openfgav1.ObjectRelation, string, string) {
	var userFilter []*openfgav1.ObjectRelation
	var relationFilter, objectType string

	// Should this actually be looking at the node we're heading towards?
	switch req.weightedEdge.GetEdgeType() {
	case weightedGraph.DirectEdge:
		// the .From() for a direct edge will have a type#rel e.g. directs-employee#other_rel
		fromLabel := req.weightedEdge.GetFrom().GetLabel()
		relationFilter = tuple.GetRelation(fromLabel)                        // directs-employee#other_rel -> other_rel
		objectType = getTypeFromLabel(req.weightedEdge.GetFrom().GetLabel()) // e.g. directs-employee

		toNode := req.weightedEdge.GetTo()

		// e.g. 'user:*'
		if toNode.GetNodeType() == weightedGraph.SpecificTypeWildcard {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: toNode.GetLabel(), // e.g. "employee:*"
			})
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
		}
	case weightedGraph.TTUEdge:
		//relationFilter = tuple.GetRelation(req.weightedEdge.GetTuplesetRelation())
		relationFilter = tuple.GetRelation(req.weightedEdge.GetTo().GetLabel())
		objectType = getTypeFromLabel(req.weightedEdge.GetTo().GetLabel())
		// a TTU edge can only have a userset as a source node
		// e.g. 'group:eng#member'
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: val.ObjectRelation.GetObject(),
			})
		} else {
			panic("unexpected source for reverse expansion of tuple to userset")
		}
	default:
		panic("unsupported edge type")
	}

	return userFilter, relationFilter, objectType
}

// expects a "type#rel".
func getTypeFromLabel(label string) string {
	userObject, _ := tuple.SplitObjectRelation(label)
	return userObject
}
*/
