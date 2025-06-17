package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const (
	listObjectsResultChannelLength = 100
)

type typeRelEntry struct {
	typeRel string // e.g. "organization#admin"

	// Only present for userset relations. Will be the userset relation string itself.
	// For `rel admin: [team#member]`, usersetRelation is "member"
	usersetRelation string

	isRecursive bool
}

// relationStack represents the path of queryable relationships encountered on the way to a terminal type.
// As reverseExpand traverses from a requested type#rel to its leaf nodes, it pushes to this stack.
// Each entry is a `typeRelEntry` struct, which contains not only the `type#relation`
// but also crucial metadata:
//   - `usersetRelation`: To handle transitions through usersets (e.g. `[team#member]`).
//   - `isRecursive`: To correctly process recursive relationship definitions (e.g. `define member: [user] or group#member`).
//
// After reaching a leaf, this stack is consumed by the `queryForTuples` function to build the precise chain of
// database queries needed to find the resulting objects.
// To avoid races, every leaf node receives its own copy of the stack.
type relationStack []typeRelEntry

func (r *relationStack) Push(value typeRelEntry) {
	*r = append(*r, value)
}

func (r *relationStack) Pop() typeRelEntry {
	element := (*r)[len(*r)-1]
	*r = (*r)[0 : len(*r)-1]
	return element
}

func (r *relationStack) Peek() typeRelEntry {
	element := (*r)[len(*r)-1]
	return element
}

func (r *relationStack) Copy() []typeRelEntry {
	dst := make(relationStack, len(*r)) // Create a new slice with the same length
	copy(dst, *r)
	return dst
}

// loopOverWeightedEdges iterates over a set of weightedGraphEdges and acts as a dispatcher,
// processing each edge according to its type to continue the reverse expansion process.
//
// While traversing, loopOverWeightedEdges appends relation entries to a stack for use in querying after traversal is complete.
// It will continue to dispatch and traverse the graph until it reaches a DirectEdge, which
// leads to a leaf node in the authorization graph. Once a DirectEdge is found, loopOverWeightedEdges invokes
// queryForTuples, passing it the stack of relations it constructed on the way to that particular leaf.
//
// For each edge, it creates a new ReverseExpandRequest, preserving the context of the overall query
// but updating the traversal state (the 'stack') based on the edge being processed.
//
// The behavior is determined by the edge type:
//
//   - DirectEdge: This represents a direct path to data. Here we initiate a call to
//     `queryForTuples` to query the datastore for tuples that match the relationship path
//     accumulated in the stack. This is the end of the traversal.
//
//   - ComputedEdge, RewriteEdge, and TTUEdge: These represent indirections in the authorization model.
//     The function modifies the traversal 'stack' to reflect the next relationship that needs to be resolved.
//     It then calls `dispatch` to continue traversing the graph with this new state until it reaches a DirectEdge.
func (c *ReverseExpandQuery) loopOverWeightedEdges(
	ctx context.Context,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	req *ReverseExpandRequest,
	resolutionMetadata *ResolutionMetadata,
	resultChan chan<- *ReverseExpandResult,
	sourceUserType string, // check to see if we can get away not using this
) error {
	pool := concurrency.NewPool(ctx, 1) // TODO: this is not a real value

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

		toNode := edge.GetTo()

		// Going to a userset presents risk of infinite loop. Using from + to ensures
		// we don't traverse the exact same edge more than once.
		goingToUserset := toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation
		if goingToUserset {
			key := edge.GetFrom().GetUniqueLabel() + toNode.GetUniqueLabel()
			if _, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{}); loaded {
				// we've already visited this userset through this edge, exit to avoid an infinite cycle
				continue
			}
		}

		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			if goingToUserset {
				// Attach the userset relation to the previous stack entry
				//  type team:
				//		define member: [user]
				//	type org:
				//		define teammate: [team#member]
				// A direct edge here is org#teammate --> team#member
				// so if we find team:fga for this user, we need to know to check for
				// team:fga#member when we check org#teammate
				r.stack[len(r.stack)-1].usersetRelation = tuple.GetRelation(toNode.GetUniqueLabel())

				// We also need to check if this userset is recursive
				// e.g. `define member: [user] or team#member`
				wt, _ := edge.GetWeight(req.User.GetObjectType())
				if wt == weightedGraph.Infinite {
					r.stack[len(r.stack)-1].isRecursive = true
				}

				r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})

				// Now continue traversing
				err := c.dispatch(ctx, r, resultChan, false, resolutionMetadata)
				if err != nil {
					errs = errors.Join(errs, err)
					return errs
				}
				continue
			}

			// We have reached a leaf node in the graph (e.g. `user` or `user:*`),
			// and the traversal for this path is complete. Now we use the stack of relations
			// we've built to query the datastore for matching tuples.
			pool.Go(func(ctx context.Context) error {
				return c.queryForTuples(
					ctx,
					r,
					resultChan,
				)
			})
		case weightedGraph.ComputedEdge:
			// A computed edge is an alias (e.g., `define viewer: editor`).
			// We replace the current relation on the stack (`viewer`) with the computed one (`editor`),
			// as tuples are only written against `editor`.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				_ = r.stack.Pop()
				r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})
			}

			err := c.dispatch(ctx, r, resultChan, false, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.TTUEdge:
			// Replace the existing type#rel on the stack with the tuple-to-userset relation:
			//
			// 	type document
			//		define parent: [folder]
			//		define viewer: admin from parent
			//
			// We need to remove document#viewer from the stack and replace it with the tupleset relation (`document#parent`).
			// Then we have to add the .To() relation `folder#admin`.
			// The stack becomes `[document#parent, folder#admin]`, and on evaluation we will first
			// query for folder#admin, then if folders exist we will see if they are related to
			// any documents as #parent.
			_ = r.stack.Pop()

			tuplesetRel := typeRelEntry{typeRel: edge.GetTuplesetRelation()}
			weight, _ := edge.GetWeight(req.User.GetObjectType())
			if weight == weightedGraph.Infinite {
				tuplesetRel.isRecursive = true
			}
			// Push tupleset relation (`document#parent`)
			r.stack.Push(tuplesetRel)
			// Push target type#rel (`folder#admin`)
			r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})

			err := c.dispatch(ctx, r, resultChan, false, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.RewriteEdge:
			// Behaves just like ComputedEdge above
			// Operator nodes (union, intersection, exclusion) are not real types, they never get added
			// to the stack.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				_ = r.stack.Pop()
				r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				err := c.dispatch(ctx, r, resultChan, false, resolutionMetadata)
				if err != nil {
					errs = errors.Join(errs, err)
					return errs
				}
			} else {
				switch toNode.GetLabel() {
				case weightedGraph.IntersectionOperator:
					err := c.intersectionHandler(ctx, r, resultChan, toNode.GetUniqueLabel(), sourceUserType, resolutionMetadata)
					if err != nil {
						return err
					}
				case weightedGraph.ExclusionOperator:
					err := c.exclusionHandler(ctx, r, resultChan, toNode.GetUniqueLabel(), sourceUserType, resolutionMetadata)
					if err != nil {
						return err
					}
				default:
					err := c.dispatch(ctx, r, resultChan, false, resolutionMetadata)
					if err != nil {
						errs = errors.Join(errs, err)
						return errs
					}
				}
			}

		default:
			panic("unsupported edge type")
		}
	}

	return errors.Join(errs, pool.Wait())
}

// queryForTuples performs all datastore-related reverse expansion logic. After a leaf node has been found in loopOverWeightedEdges,
// this function works backwards from a specified user (using the stack created in loopOverWeightedEdges)
// and an initial relationship edge to find all the objects that the given user has the given relationship with.
// The function defines a recursive inner function, `queryFunc`, which is executed concurrently for different
// branches of the relationship graph.
//
// On its initial execution, it constructs a database query filter based on the starting user and the "To"
// part of the initial DirectEdge, which can be a direct user, a wildcard user, or a userset.
// In subsequent recursive calls, it takes a `foundObject`—the object found in the previous step—and
// that foundObject becomes the 'user' in the next query in the stack. Take this model for example:
//
//		type user
//		type organization
//		  relations
//			define member: [user]
//			define repo_admin: [organization#member]
//		type repo
//		  relations
//	        define admin: repo_admin from owner
//	        define owner: [organization]
//
// When searching for repos which user:bob has #admin relation to, queryFunc behaves like so:
//
//  1. Search for organizations where user:bob is a member. We find this tuple: organization:fga#member@user:bob
//  2. Take that foundObject, `organization:fga` and pass it to the next call of queryFunc.
//  3. Query for tuples matching `organization#repo_admin@organization:fga#member` (because this is a userset relation).
//  4. If we found another object in step 3, pass that into the next queryFunc call to be evaluated against the next element in the stack.
//
// We continue doing this recursively until we hit one of the two cases below:
//
//  1. We cannot locate a tuple for a query—this means this branch of the tree yielded no results.
//  2. The stack is empty—this means there are no more queries to run, and this object is a candidate to be returned
//     to ListObjects through resultChan.
func (c *ReverseExpandQuery) queryForTuples(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
) error {
	// TODO: don't forget telemetry
	var wg sync.WaitGroup
	errChan := make(chan error, 100) // TODO: random value here, gotta do this another way

	// This map is used for memoization within this query path. It prevents re-running the exact
	// same database query for a given object type, relation, and user filter.
	jobDedupeMap := new(sync.Map)
	var queryFunc func(context.Context, *ReverseExpandRequest, string)
	numJobs := atomic.Int32{} // TODO: remove, this is for debugging
	queryFunc = func(qCtx context.Context, r *ReverseExpandRequest, foundObject string) {
		defer wg.Done()
		if qCtx.Err() != nil {
			return
		}
		numJobs.Add(1)

		var typeRel string
		var userFilter []*openfgav1.ObjectRelation

		// This is true on every call except the first
		if foundObject != "" {
			entry := r.stack.Peek()
			// For recursive relations, don't actually pop the relation off the stack.
			if !entry.isRecursive {
				// If it is *not* recursive (most cases), remove the last element
				r.stack.Pop()
			}
			typeRel = entry.typeRel
			filter := &openfgav1.ObjectRelation{Object: foundObject}
			if entry.usersetRelation != "" {
				filter.Relation = entry.usersetRelation
			}
			userFilter = append(userFilter, filter)
		} else {
			// this else block ONLY hits on the first call
			var userID string
			// TODO: we might be able to handle pure userset queries now
			// We will always have a UserRefObject here. Queries that come in for pure usersets do not take this code path.
			// e.g. ListObjects(team:fga#member, document, viewer) will not make it here.
			if val, ok := req.User.(*UserRefObject); ok {
				userID = val.Object.GetId()
			}

			to := req.weightedEdge.GetTo()

			switch to.GetNodeType() {
			case weightedGraph.SpecificType: // Direct User Reference. To() -> "user"
				typeRel = req.stack.Pop().typeRel
				userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: tuple.BuildObject(to.GetUniqueLabel(), userID)})

			case weightedGraph.SpecificTypeWildcard: // Wildcard Reference To() -> "user:*"
				typeRel = req.stack.Pop().typeRel
				userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: to.GetUniqueLabel()})
			}
		}

		objectType, relation := tuple.SplitObjectRelation(typeRel)

		// TODO: polish this bit
		// Create a unique key for the current query to avoid duplicate work.
		key := utils.Reduce(userFilter, "", func(accumulator string, current *openfgav1.ObjectRelation) string {
			return current.String() + accumulator
		})
		key += relation + objectType
		if _, loaded := jobDedupeMap.LoadOrStore(key, struct{}{}); loaded {
			// If this exact query has been run before in this path, abort.
			return
		}

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
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				errChan <- err
				break LoopOnIterator
			}

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

			foundObject = tk.GetObject() // This will be a "type:id" e.g. "document:roadmap"

			// If there are no more type#rel to look for in the stack that means we have hit the base case
			// and this object is a candidate for return to the user.
			if len(r.stack) == 0 {
				_ = c.trySendCandidate(ctx, false, foundObject, resultChan)
				continue
			}

			// This is the logic for handling recursive relations, such as `define member: [user] or group#member`.
			// When we find a tuple related to a recursive relation, we need to explore two paths concurrently:
			// 1. Continue the recursion: The user in the found tuple might be a member of another group.
			// 2. Exit the recursion: The object of the found tuple might satisfy the next relation in the stack.
			if r.stack.Peek().isRecursive {
				// If the recursive relation is the last one in the stack, foundObject could be a final result.
				if len(r.stack) == 1 && tuple.GetType(tk.GetObject()) == req.ObjectType {
					_ = c.trySendCandidate(ctx, false, foundObject, resultChan)
				}

				// Path 1: Continue the recursive search.
				// We call `queryFunc` again with the same request (`r`), which keeps the recursive
				// relation on the stack, to find further nested relationships.
				wg.Add(1)
				go queryFunc(qCtx, r, foundObject)

				// Path 2: Exit the recursion and move up the hierarchy.
				// This is only possible if there are other relations higher up in the stack.
				if len(r.stack) > 1 {
					// Create a new request, pop the recursive relation off its stack, and then
					// call `queryFunc`. This explores whether the `foundObject` satisfies the next
					// relationship in the original query chain.
					newReq := r.clone()
					_ = newReq.stack.Pop()
					wg.Add(1)
					go queryFunc(qCtx, newReq, foundObject)
				}
			} else {
				// For non-recursive relations (majority of cases), if there are more items on the stack, we continue
				// the evaluation one level higher up the tree with the `foundObject`.
				wg.Add(1)
				go queryFunc(qCtx, r.clone(), foundObject)
			}
		}
	}

	// Now kick off the recursive function defined above.
	wg.Add(1)
	go queryFunc(ctx, req, "")

	wg.Wait()
	fmt.Printf("JUstin ran %d jobs\n", numJobs.Load())
	close(errChan)
	var errs error
	for err := range errChan {
		errs = errors.Join(errs, err)
	}
	return errs
}

// invoke loopOverWeightedEdges to get list objects candidate. Check
// will then be invoked on the non-lowest weight edges against these
// list objects candidates. If check returns true, then the list
// object candidates are true candidates and will be returned via
// resultChan. If check returns false, then these list object candidates
// are invalid because it does not satisfy all paths for intersection.
func (c *ReverseExpandQuery) intersectionHandler(ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	uniqueLabel string,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata) error {
	tmpResultChan := make(chan *ReverseExpandResult, listObjectsResultChannelLength)

	intersectionEdgeComparison, err := c.typesystem.GetLowestEdgesAndTheirSiblingsForIntersection(uniqueLabel, sourceUserType)
	if err != nil {
		c.logger.Error("Failed to get edges from weighted graph",
			zap.Error(err),
			zap.String("uniqueLabel", uniqueLabel),
			zap.String("sourceUserType", sourceUserType))
		return err
	}
	if intersectionEdgeComparison == nil {
		// This means that there are edges with no connection. It can never be
		// evaluated to true.
		return nil
	}

	var lowestWeightEdges []*weightedGraph.WeightedAuthorizationModelEdge
	if intersectionEdgeComparison.DirectEdgesAreLeastWeight {
		lowestWeightEdges = intersectionEdgeComparison.DirectEdges
	} else {
		lowestWeightEdges = []*weightedGraph.WeightedAuthorizationModelEdge{intersectionEdgeComparison.LowestEdge}
	}

	pool := concurrency.NewPool(ctx, 2)
	// getting list object candidates from the lowest weight edge and have its result
	// pass through tmpResultChan.
	pool.Go(func(ctx context.Context) error {
		defer close(tmpResultChan)
		newReq := &ReverseExpandRequest{
			StoreID:          req.StoreID,
			Consistency:      req.Consistency,
			Context:          req.Context,
			ContextualTuples: req.ContextualTuples,
			User:             req.User,
			stack:            req.stack.Copy(),
			weightedEdge:     req.weightedEdge,
			ObjectType:       req.ObjectType,
			Relation:         req.Relation,
		}
		return c.shallowClone().loopOverWeightedEdges(ctx, lowestWeightEdges, newReq, resolutionMetadata, tmpResultChan, sourceUserType)
	})

	siblings := intersectionEdgeComparison.Siblings
	usersets := make([]*openfgav1.Userset, 0, len(siblings)+1)

	if !intersectionEdgeComparison.DirectEdgesAreLeastWeight && len(intersectionEdgeComparison.DirectEdges) > 0 {
		// direct weight is not the lowest edge. Therefore, need to call check against directly assigned types.
		usersets = append(usersets, typesystem.This())
	}

	for _, sibling := range siblings {
		userset, err := c.typesystem.ConstructUserset(ctx, sibling.GetEdgeType(), sibling.GetTo().GetUniqueLabel())
		if err != nil {
			c.logger.Error("Failed to construct userset",
				zap.String("function", "intersectionHandler"),
				zap.Int64("EdgeType", int64(sibling.GetEdgeType())),
				zap.String("UniqueLabel", sibling.GetTo().GetUniqueLabel()),
				zap.Error(err))
			return err
		}
		usersets = append(usersets, userset)
	}

	// Calling check on list objects candidates against non lowest weight edges
	pool.Go(func(ctx context.Context) error {
		for tmpResult := range tmpResultChan {
			handlerFunc := c.localCheckResolver.CheckSetOperation(ctx,
				&graph.ResolveCheckRequest{
					StoreID:              req.StoreID,
					AuthorizationModelID: c.typesystem.GetAuthorizationModelID(),
					TupleKey:             tuple.NewTupleKey(tmpResult.Object, req.Relation, req.User.String()),
					ContextualTuples:     req.ContextualTuples,
					Context:              req.Context,
					Consistency:          req.Consistency,
					RequestMetadata:      graph.NewCheckRequestMetadata(),
				}, graph.IntersectionSetOperator, graph.Intersection, usersets...)
			tmpCheckResult, err := handlerFunc(ctx)
			if err != nil {
				c.logger.Error("Failed to execute", zap.Error(err),
					zap.String("function", "intersectionHandler"),
					zap.String("object", tmpResult.Object),
					zap.String("relation", req.Relation),
					zap.String("user", req.User.String()))
				return err
			}

			if tmpCheckResult.GetAllowed() {
				// check returns true. Hence, candidates are true candidate and can be passed back to its parents.
				err = c.trySendCandidate(ctx, false, tmpResult.Object, resultChan)
				if err != nil {
					return err
				}
			}
			// otherwise, candidates are not true candidate and no need to pass back to its parents.
		}
		return nil
	})

	return pool.Wait()
}

// invoke loopOverWeightedEdges to get list objects candidate. Check
// will then be invoked on the excluded edge against these
// list objects candidates. If check returns false, then the list
// object candidates are true candidates and will be returned via
// resultChan. If check returns true, then these list object candidates
// are invalid because it does not satisfy all paths for exclusion.
func (c *ReverseExpandQuery) exclusionHandler(ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	uniqueLabel string,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata) error {
	baseEdges, excludedEdge, err := c.typesystem.GetLowestWeightEdgeForExclusion(uniqueLabel, sourceUserType)
	if err != nil {
		c.logger.Error("Failed to get lowest weight edge for exclusionHandler", zap.Error(err),
			zap.String("uniqueLabel", uniqueLabel),
			zap.String("sourceUserType", sourceUserType))
		return err
	}

	// We do not have to check whether baseEdges have length 0 because it would have been
	// thrown out by GetEdgesFromWeightedGraph and this function will not be executed.
	tmpResultChan := make(chan *ReverseExpandResult, listObjectsResultChannelLength)
	pool := concurrency.NewPool(ctx, 2)
	pool.Go(func(ctx context.Context) error {
		defer close(tmpResultChan)
		newReq := &ReverseExpandRequest{
			StoreID:          req.StoreID,
			Consistency:      req.Consistency,
			Context:          req.Context,
			ContextualTuples: req.ContextualTuples,
			User:             req.User,
			stack:            req.stack.Copy(),
			weightedEdge:     req.weightedEdge, // Inherited but not used by the override path
			ObjectType:       req.ObjectType,
			Relation:         req.Relation,
		}
		return c.shallowClone().loopOverWeightedEdges(ctx, baseEdges, newReq, resolutionMetadata, tmpResultChan, sourceUserType)
	})

	userset, err := c.typesystem.ConstructUserset(ctx, excludedEdge.GetEdgeType(), excludedEdge.GetTo().GetUniqueLabel())
	if err != nil {
		c.logger.Error("Failed to construct userset",
			zap.Error(err),
			zap.String("function", "exclusionHandler"),
			zap.Int64("edgeType", int64(excludedEdge.GetEdgeType())),
			zap.String("uniqueLabel", excludedEdge.GetTo().GetUniqueLabel()))
		return err
	}
	pool.Go(func(ctx context.Context) error {
		for tmpResult := range tmpResultChan {
			handlerFunc := c.localCheckResolver.CheckRewrite(ctx,
				&graph.ResolveCheckRequest{
					StoreID:              req.StoreID,
					AuthorizationModelID: c.typesystem.GetAuthorizationModelID(),
					TupleKey:             tuple.NewTupleKey(tmpResult.Object, req.Relation, req.User.String()),
					ContextualTuples:     req.ContextualTuples,
					Context:              req.Context,
					Consistency:          req.Consistency,
					RequestMetadata:      graph.NewCheckRequestMetadata(),
				}, userset)
			tmpCheckResult, err := handlerFunc(ctx)
			if err != nil {
				c.logger.Error("Failed to execute", zap.Error(err),
					zap.String("function", "exclusionHandler"),
					zap.String("object", tmpResult.Object),
					zap.String("relation", req.Relation),
					zap.String("user", req.User.String()))
				return err
			}
			if !tmpCheckResult.GetAllowed() {
				err = c.trySendCandidate(ctx, false, tmpResult.Object, resultChan)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	return pool.Wait()
}
