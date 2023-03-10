#include "planner/join_order_enumerator.h"

#include "planner/logical_plan/logical_operator/logical_cross_product.h"
#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "planner/logical_plan/logical_operator/logical_ftable_scan.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/projection_planner.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

std::vector<std::unique_ptr<LogicalPlan>> JoinOrderEnumerator::enumerate(
    const QueryGraphCollection& queryGraphCollection, expression_vector& predicates) {
    assert(queryGraphCollection.getNumQueryGraphs() > 0);
    // project predicates on each query graph
    std::vector<expression_vector> predicatesToPushDownPerGraph;
    predicatesToPushDownPerGraph.resize(queryGraphCollection.getNumQueryGraphs());
    expression_vector predicatesToPullUp;
    for (auto& predicate : predicates) {
        bool needToPullUp = true;
        for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
            auto queryGraph = queryGraphCollection.getQueryGraph(i);
            if (queryGraph->canProjectExpression(predicate.get())) {
                predicatesToPushDownPerGraph[i].push_back(predicate);
                needToPullUp = false;
            }
        }
        if (needToPullUp) {
            predicatesToPullUp.push_back(predicate);
        }
    }
    // enumerate plans for each query graph
    std::vector<std::vector<std::unique_ptr<LogicalPlan>>> plansPerQueryGraph;
    for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
        plansPerQueryGraph.push_back(
            enumerate(queryGraphCollection.getQueryGraph(i), predicatesToPushDownPerGraph[i]));
    }
    // take cross products
    auto result = std::move(plansPerQueryGraph[0]);
    for (auto i = 1u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
        result = planCrossProduct(std::move(result), std::move(plansPerQueryGraph[i]));
    }
    // apply remaining predicates
    for (auto& plan : result) {
        for (auto& predicate : predicatesToPullUp) {
            queryPlanner->appendFilter(predicate, *plan);
        }
    }
    return result;
}

std::vector<std::unique_ptr<LogicalPlan>> JoinOrderEnumerator::planCrossProduct(
    std::vector<std::unique_ptr<LogicalPlan>> leftPlans,
    std::vector<std::unique_ptr<LogicalPlan>> rightPlans) {
    std::vector<std::unique_ptr<LogicalPlan>> result;
    for (auto& leftPlan : leftPlans) {
        for (auto& rightPlan : rightPlans) {
            auto leftPlanCopy = leftPlan->shallowCopy();
            auto rightPlanCopy = rightPlan->shallowCopy();
            appendCrossProduct(*leftPlanCopy, *rightPlanCopy);
            result.push_back(std::move(leftPlanCopy));
        }
    }
    return result;
}

std::vector<std::unique_ptr<LogicalPlan>> JoinOrderEnumerator::enumerate(
    QueryGraph* queryGraph, expression_vector& predicates) {
    context->init(queryGraph, predicates);
    assert(context->expressionsToScanFromOuter.empty());
    planTableScan();
    context->currentLevel++;
    while (context->currentLevel < context->maxLevel) {
        planLevel(context->currentLevel++);
    }
    return std::move(context->getPlans(context->getFullyMatchedSubqueryGraph()));
}

std::unique_ptr<JoinOrderEnumeratorContext> JoinOrderEnumerator::enterSubquery(
    LogicalPlan* outerPlan, expression_vector expressionsToScan,
    expression_vector nodeIDsToScanFromInnerAndOuter) {
    auto prevContext = std::move(context);
    context = std::make_unique<JoinOrderEnumeratorContext>();
    context->outerPlan = outerPlan;
    context->expressionsToScanFromOuter = std::move(expressionsToScan);
    context->nodeIDsToScanFromInnerAndOuter = std::move(nodeIDsToScanFromInnerAndOuter);
    return prevContext;
}

void JoinOrderEnumerator::exitSubquery(std::unique_ptr<JoinOrderEnumeratorContext> prevContext) {
    context = std::move(prevContext);
}

void JoinOrderEnumerator::planLevel(uint32_t level) {
    assert(level > 1);
    if (level > MAX_LEVEL_TO_PLAN_EXACTLY) {
        planLevelApproximately(level);
    } else {
        planLevelExactly(level);
    }
    context->subPlansTable->finalizeLevel(level);
}

void JoinOrderEnumerator::planLevelExactly(uint32_t level) {
    auto maxLeftLevel = floor(level / 2.0);
    for (auto leftLevel = 1u; leftLevel <= maxLeftLevel; ++leftLevel) {
        auto rightLevel = level - leftLevel;
        if (leftLevel > 1) { // wcoj requires at least 2 rels
            planWCOJoin(leftLevel, rightLevel);
        }
        planInnerJoin(leftLevel, rightLevel);
    }
}

void JoinOrderEnumerator::planLevelApproximately(uint32_t level) {
    planInnerJoin(1, level - 1);
}

void JoinOrderEnumerator::planTableScan() {
    auto queryGraph = context->getQueryGraph();
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes(); ++nodePos) {
        planNodeScan(nodePos);
    }
    for (auto relPos = 0u; relPos < queryGraph->getNumQueryRels(); ++relPos) {
        planRelScan(relPos);
    }
}

static bool isPrimaryPropertyAndLiteralPair(const Expression& left, const Expression& right) {
    if (left.expressionType != PROPERTY || right.expressionType != LITERAL) {
        return false;
    }
    auto propertyExpression = (const PropertyExpression&)left;
    return propertyExpression.isPrimaryKey();
}

static bool isIndexScanExpression(Expression& expression) {
    if (expression.expressionType != EQUALS) { // check equality comparison
        return false;
    }
    auto left = expression.getChild(0);
    auto right = expression.getChild(1);
    if (isPrimaryPropertyAndLiteralPair(*left, *right) ||
        isPrimaryPropertyAndLiteralPair(*right, *left)) {
        return true;
    }
    return false;
}

static std::shared_ptr<Expression> extractIndexExpression(Expression& expression) {
    if (expression.getChild(0)->expressionType == LITERAL) {
        return expression.getChild(0);
    }
    return expression.getChild(1);
}

static std::pair<std::shared_ptr<Expression>, expression_vector> splitIndexAndPredicates(
    const expression_vector& predicates) {
    std::shared_ptr<Expression> indexExpression;
    expression_vector predicatesToApply;
    for (auto& predicate : predicates) {
        if (isIndexScanExpression(*predicate)) {
            indexExpression = extractIndexExpression(*predicate);
        } else {
            predicatesToApply.push_back(predicate);
        }
    }
    return make_pair(indexExpression, predicatesToApply);
}

void JoinOrderEnumerator::planNodeScan(uint32_t nodePos) {
    auto node = context->queryGraph->getQueryNode(nodePos);
    auto newSubgraph = context->getEmptySubqueryGraph();
    newSubgraph.addQueryNode(nodePos);
    auto plan = std::make_unique<LogicalPlan>();
    auto predicates = getNewlyMatchedExpressions(
        context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
    // In un-nested subquery, e.g. MATCH (a) OPTIONAL MATCH (a)-[e1]->(b), the inner query
    // ("(a)-[e1]->(b)") needs to scan a, which is already scanned in the outer query (a). To avoid
    // scanning storage twice, we keep track of node table "a" and make sure when planning inner
    // query, we only scan internal ID of "a".
    if (!context->nodeToScanFromInnerAndOuter(node.get())) {
        std::shared_ptr<Expression> indexExpression = nullptr;
        expression_vector predicatesToApply = predicates;
        if (!node->isMultiLabeled()) { // check for index scan
            auto [_indexExpression, _predicatesToApply] = splitIndexAndPredicates(predicates);
            indexExpression = _indexExpression;
            predicatesToApply = _predicatesToApply;
        }
        if (indexExpression != nullptr) {
            appendIndexScanNode(node, indexExpression, *plan);
        } else {
            appendScanNode(node, *plan);
        }
        planFiltersForNode(predicatesToApply, node, *plan);
        planPropertyScansForNode(node, *plan);
    } else {
        appendScanNode(node, *plan);
    }
    context->addPlan(newSubgraph, std::move(plan));
}

void JoinOrderEnumerator::planFiltersForNode(
    expression_vector& predicates, std::shared_ptr<NodeExpression> node, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        auto propertiesToScan = getPropertiesForVariable(*predicate, *node);
        queryPlanner->appendScanNodePropIfNecessary(propertiesToScan, node, plan);
        queryPlanner->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::planPropertyScansForNode(
    std::shared_ptr<NodeExpression> node, LogicalPlan& plan) {
    auto properties = queryPlanner->getPropertiesForNode(*node);
    queryPlanner->appendScanNodePropIfNecessary(properties, node, plan);
}

static std::pair<std::shared_ptr<NodeExpression>, std::shared_ptr<NodeExpression>>
getBoundAndNbrNodes(const RelExpression& rel, RelDirection direction) {
    auto boundNode = direction == FWD ? rel.getSrcNode() : rel.getDstNode();
    auto dstNode = direction == FWD ? rel.getDstNode() : rel.getSrcNode();
    return make_pair(boundNode, dstNode);
}

void JoinOrderEnumerator::planRelScan(uint32_t relPos) {
    auto rel = context->queryGraph->getQueryRel(relPos);
    auto newSubgraph = context->getEmptySubqueryGraph();
    newSubgraph.addQueryRel(relPos);
    auto predicates = getNewlyMatchedExpressions(
        context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
    for (auto direction : REL_DIRECTIONS) {
        auto plan = std::make_unique<LogicalPlan>();
        auto [boundNode, _] = getBoundAndNbrNodes(*rel, direction);
        appendScanNode(boundNode, *plan);
        planExtendAndFilters(rel, direction, predicates, *plan);
        context->addPlan(newSubgraph, std::move(plan));
    }
}

void JoinOrderEnumerator::planExtendAndFilters(std::shared_ptr<RelExpression> rel,
    RelDirection direction, expression_vector& predicates, LogicalPlan& plan) {
    auto [boundNode, dstNode] = getBoundAndNbrNodes(*rel, direction);
    auto properties = queryPlanner->getPropertiesForRel(*rel);
    appendExtend(boundNode, dstNode, rel, direction, properties, plan);
    for (auto& predicate : predicates) {
        queryPlanner->appendFilter(predicate, plan);
    }
}

static std::unordered_map<uint32_t, std::vector<std::shared_ptr<RelExpression>>>
populateIntersectRelCandidates(const QueryGraph& queryGraph, const SubqueryGraph& subgraph) {
    std::unordered_map<uint32_t, std::vector<std::shared_ptr<RelExpression>>>
        intersectNodePosToRelsMap;
    for (auto relPos : subgraph.getRelNbrPositions()) {
        auto rel = queryGraph.getQueryRel(relPos);
        if (!queryGraph.containsQueryNode(rel->getSrcNodeName()) ||
            !queryGraph.containsQueryNode(rel->getDstNodeName())) {
            continue;
        }
        auto srcNodePos = queryGraph.getQueryNodePos(rel->getSrcNodeName());
        auto dstNodePos = queryGraph.getQueryNodePos(rel->getDstNodeName());
        auto isSrcConnected = subgraph.queryNodesSelector[srcNodePos];
        auto isDstConnected = subgraph.queryNodesSelector[dstNodePos];
        // Closing rel should be handled with inner join.
        if (isSrcConnected && isDstConnected) {
            continue;
        }
        auto intersectNodePos = isSrcConnected ? dstNodePos : srcNodePos;
        if (!intersectNodePosToRelsMap.contains(intersectNodePos)) {
            intersectNodePosToRelsMap.insert(
                {intersectNodePos, std::vector<std::shared_ptr<RelExpression>>{}});
        }
        intersectNodePosToRelsMap.at(intersectNodePos).push_back(rel);
    }
    return intersectNodePosToRelsMap;
}

void JoinOrderEnumerator::planWCOJoin(uint32_t leftLevel, uint32_t rightLevel) {
    assert(leftLevel <= rightLevel);
    auto queryGraph = context->getQueryGraph();
    for (auto& rightSubgraph : context->subPlansTable->getSubqueryGraphs(rightLevel)) {
        auto candidates = populateIntersectRelCandidates(*queryGraph, rightSubgraph);
        for (auto& [intersectNodePos, rels] : candidates) {
            if (rels.size() == leftLevel) {
                auto intersectNode = queryGraph->getQueryNode(intersectNodePos);
                planWCOJoin(rightSubgraph, rels, intersectNode);
            }
        }
    }
}

// As a heuristic for wcoj, we always pick rel scan that starts from the bound node.
static std::unique_ptr<LogicalPlan> getWCOJBuildPlanForRel(
    std::vector<std::unique_ptr<LogicalPlan>>& candidatePlans, const NodeExpression& boundNode) {
    std::unique_ptr<LogicalPlan> result;
    for (auto& candidatePlan : candidatePlans) {
        if (LogicalPlanUtil::getSequentialNode(*candidatePlan)->getUniqueName() ==
            boundNode.getUniqueName()) {
            assert(result == nullptr);
            result = candidatePlan->shallowCopy();
        }
    }
    return result;
}

void JoinOrderEnumerator::planWCOJoin(const SubqueryGraph& subgraph,
    std::vector<std::shared_ptr<RelExpression>> rels,
    const std::shared_ptr<NodeExpression>& intersectNode) {
    auto newSubgraph = subgraph;
    std::vector<SubqueryGraph> prevSubgraphs;
    prevSubgraphs.push_back(subgraph);
    expression_vector boundNodeIDs;
    std::vector<std::unique_ptr<LogicalPlan>> relPlans;
    for (auto& rel : rels) {
        auto boundNode = rel->getSrcNodeName() == intersectNode->getUniqueName() ?
                             rel->getDstNode() :
                             rel->getSrcNode();
        boundNodeIDs.push_back(boundNode->getInternalIDProperty());
        auto relPos = context->getQueryGraph()->getQueryRelPos(rel->getUniqueName());
        auto prevSubgraph = context->getEmptySubqueryGraph();
        prevSubgraph.addQueryRel(relPos);
        prevSubgraphs.push_back(subgraph);
        newSubgraph.addQueryRel(relPos);
        // fetch build plans for rel
        auto relSubgraph = context->getEmptySubqueryGraph();
        relSubgraph.addQueryRel(relPos);
        assert(context->subPlansTable->containSubgraphPlans(relSubgraph));
        auto& relPlanCandidates = context->subPlansTable->getSubgraphPlans(relSubgraph);
        assert(relPlanCandidates.size() == 2); // 2 directions
        relPlans.push_back(getWCOJBuildPlanForRel(relPlanCandidates, *boundNode));
    }
    auto predicates =
        getNewlyMatchedExpressions(prevSubgraphs, newSubgraph, context->getWhereExpressions());
    for (auto& leftPlan : context->getPlans(subgraph)) {
        auto leftPlanCopy = leftPlan->shallowCopy();
        std::vector<std::unique_ptr<LogicalPlan>> rightPlansCopy;
        for (auto& relPlan : relPlans) {
            rightPlansCopy.push_back(relPlan->shallowCopy());
        }
        appendIntersect(
            intersectNode->getInternalIDProperty(), boundNodeIDs, *leftPlanCopy, rightPlansCopy);
        for (auto& predicate : predicates) {
            queryPlanner->appendFilter(predicate, *leftPlanCopy);
        }
        context->subPlansTable->addPlan(newSubgraph, std::move(leftPlanCopy));
    }
}

// E.g. Query graph (a)-[e1]->(b), (b)-[e2]->(a) and join between (a)-[e1] and [e2]
// Since (b) is not in the scope of any join subgraph, join node is analyzed as (a) only, However,
// [e1] and [e2] are also connected at (b) implicitly. So actual join nodes should be (a) and (b).
// We prune such join.
// Note that this does not mean we may lose good plan. An equivalent join can be found between [e2]
// and (a)-[e1]->(b).
static bool needPruneImplicitJoins(
    const SubqueryGraph& leftSubgraph, const SubqueryGraph& rightSubgraph, uint32_t numJoinNodes) {
    auto leftNodePositions = leftSubgraph.getNodePositionsIgnoringNodeSelector();
    auto rightNodePositions = rightSubgraph.getNodePositionsIgnoringNodeSelector();
    auto intersectionSize = 0;
    for (auto& pos : leftNodePositions) {
        if (rightNodePositions.contains(pos)) {
            intersectionSize++;
        }
    }
    return intersectionSize != numJoinNodes;
}

void JoinOrderEnumerator::planInnerJoin(uint32_t leftLevel, uint32_t rightLevel) {
    assert(leftLevel <= rightLevel);
    for (auto& rightSubgraph : context->subPlansTable->getSubqueryGraphs(rightLevel)) {
        for (auto& nbrSubgraph : rightSubgraph.getNbrSubgraphs(leftLevel)) {
            // E.g. MATCH (a)->(b) MATCH (b)->(c)
            // Since we merge query graph for multipart query, during enumeration for the second
            // match, the query graph is (a)->(b)->(c). However, we omit plans corresponding to the
            // first match (i.e. (a)->(b)).
            if (!context->containPlans(nbrSubgraph)) {
                continue;
            }
            auto joinNodePositions = rightSubgraph.getConnectedNodePos(nbrSubgraph);
            auto joinNodes = context->queryGraph->getQueryNodes(joinNodePositions);
            if (needPruneImplicitJoins(nbrSubgraph, rightSubgraph, joinNodes.size())) {
                continue;
            }
            // If index nested loop (INL) join is possible, we prune hash join plans
            if (canApplyINLJoin(rightSubgraph, nbrSubgraph, joinNodes)) {
                planInnerINLJoin(rightSubgraph, nbrSubgraph, joinNodes);
            } else if (canApplyINLJoin(nbrSubgraph, rightSubgraph, joinNodes)) {
                planInnerINLJoin(nbrSubgraph, rightSubgraph, joinNodes);
            } else {
                planInnerHashJoin(rightSubgraph, nbrSubgraph, joinNodes, leftLevel != rightLevel);
            }
        }
    }
}

// Check whether given node ID has sequential guarantee on the plan.
static bool isNodeSequential(LogicalPlan& plan, NodeExpression* node) {
    auto sequentialNode = LogicalPlanUtil::getSequentialNode(plan);
    return sequentialNode != nullptr && sequentialNode->getUniqueName() == node->getUniqueName();
}

// We apply index nested loop join if the following to conditions are satisfied
// - otherSubgraph is an edge; and
// - join node is sequential on at least one plan corresponding to subgraph. (Otherwise INLJ will
// trigger non-sequential read).
bool JoinOrderEnumerator::canApplyINLJoin(const SubqueryGraph& subgraph,
    const SubqueryGraph& otherSubgraph,
    const std::vector<std::shared_ptr<NodeExpression>>& joinNodes) {
    if (!otherSubgraph.isSingleRel() || joinNodes.size() > 1) {
        return false;
    }
    for (auto& plan : context->getPlans(subgraph)) {
        if (isNodeSequential(*plan, joinNodes[0].get())) {
            return true;
        }
    }
    return false;
}

static uint32_t extractJoinRelPos(const SubqueryGraph& subgraph, const QueryGraph& queryGraph) {
    for (auto relPos = 0u; relPos < queryGraph.getNumQueryRels(); ++relPos) {
        if (subgraph.queryRelsSelector[relPos]) {
            return relPos;
        }
    }
    throw InternalException("Cannot extract relPos.");
}

void JoinOrderEnumerator::planInnerINLJoin(const SubqueryGraph& subgraph,
    const SubqueryGraph& otherSubgraph,
    const std::vector<std::shared_ptr<NodeExpression>>& joinNodes) {
    assert(otherSubgraph.getNumQueryRels() == 1 && joinNodes.size() == 1);
    auto boundNode = joinNodes[0].get();
    auto queryGraph = context->getQueryGraph();
    auto relPos = extractJoinRelPos(otherSubgraph, *queryGraph);
    auto rel = queryGraph->getQueryRel(relPos);
    auto newSubgraph = subgraph;
    newSubgraph.addQueryRel(relPos);
    auto predicates =
        getNewlyMatchedExpressions(subgraph, newSubgraph, context->getWhereExpressions());
    for (auto& prevPlan : context->getPlans(subgraph)) {
        if (isNodeSequential(*prevPlan, boundNode)) {
            auto plan = prevPlan->shallowCopy();
            auto direction = boundNode->getUniqueName() == rel->getSrcNodeName() ? FWD : BWD;
            planExtendAndFilters(rel, direction, predicates, *plan);
            context->addPlan(newSubgraph, move(plan));
        }
    }
}

void JoinOrderEnumerator::planInnerHashJoin(const SubqueryGraph& subgraph,
    const SubqueryGraph& otherSubgraph, std::vector<std::shared_ptr<NodeExpression>> joinNodes,
    bool flipPlan) {
    auto newSubgraph = subgraph;
    newSubgraph.addSubqueryGraph(otherSubgraph);
    auto predicates =
        getNewlyMatchedExpressions(std::vector<SubqueryGraph>{subgraph, otherSubgraph}, newSubgraph,
            context->getWhereExpressions());
    for (auto& leftPlan : context->getPlans(subgraph)) {
        for (auto& rightPlan : context->getPlans(otherSubgraph)) {
            auto leftPlanProbeCopy = leftPlan->shallowCopy();
            auto rightPlanBuildCopy = rightPlan->shallowCopy();
            auto leftPlanBuildCopy = leftPlan->shallowCopy();
            auto rightPlanProbeCopy = rightPlan->shallowCopy();
            planInnerHashJoin(joinNodes, *leftPlanProbeCopy, *rightPlanBuildCopy);
            planFiltersForHashJoin(predicates, *leftPlanProbeCopy);
            context->addPlan(newSubgraph, std::move(leftPlanProbeCopy));
            // flip build and probe side to get another HashJoin plan
            if (flipPlan) {
                planInnerHashJoin(joinNodes, *rightPlanProbeCopy, *leftPlanBuildCopy);
                planFiltersForHashJoin(predicates, *rightPlanProbeCopy);
                context->addPlan(newSubgraph, std::move(rightPlanProbeCopy));
            }
        }
    }
}

void JoinOrderEnumerator::planFiltersForHashJoin(expression_vector& predicates, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        queryPlanner->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::appendScanNode(std::shared_ptr<NodeExpression>& node, LogicalPlan& plan) {
    assert(plan.isEmpty());
    auto scan = make_shared<LogicalScanNode>(node);
    scan->computeFactorizedSchema();
    // update cardinality
    auto group = scan->getSchema()->getGroup(node->getInternalIDPropertyName());
    auto numNodes = 0u;
    for (auto& tableID : node->getTableIDs()) {
        numNodes += nodesStatistics.getNodeStatisticsAndDeletedIDs(tableID)->getNumTuples();
    }
    group->setMultiplier(numNodes);
    plan.setLastOperator(std::move(scan));
}

void JoinOrderEnumerator::appendIndexScanNode(std::shared_ptr<NodeExpression>& node,
    std::shared_ptr<Expression> indexExpression, LogicalPlan& plan) {
    assert(plan.isEmpty());
    QueryPlanner::appendExpressionsScan(expression_vector{indexExpression}, plan);
    auto scan =
        make_shared<LogicalIndexScanNode>(node, std::move(indexExpression), plan.getLastOperator());
    scan->computeFactorizedSchema();
    // update cardinality
    auto group = scan->getSchema()->getGroup(node->getInternalIDPropertyName());
    group->setMultiplier(1);
    plan.setLastOperator(std::move(scan));
}

// When extend might increase cardinality (i.e. n * m), we extend to a new factorization group.
bool JoinOrderEnumerator::needExtendToNewGroup(
    RelExpression& rel, NodeExpression& boundNode, RelDirection direction) {
    auto extendToNewGroup = false;
    extendToNewGroup |= boundNode.isMultiLabeled();
    extendToNewGroup |= rel.isMultiLabeled();
    if (!rel.isMultiLabeled()) {
        auto relTableID = rel.getSingleTableID();
        extendToNewGroup |=
            !catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(relTableID, direction);
    }
    return extendToNewGroup;
}

void JoinOrderEnumerator::appendExtend(std::shared_ptr<NodeExpression> boundNode,
    std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
    RelDirection direction, const expression_vector& properties, LogicalPlan& plan) {
    auto extendToNewGroup = needExtendToNewGroup(*rel, *boundNode, direction);
    auto extend = make_shared<LogicalExtend>(
        boundNode, nbrNode, rel, direction, properties, extendToNewGroup, plan.getLastOperator());
    QueryPlanner::appendFlattens(extend->getGroupsPosToFlatten(), plan);
    extend->setChild(0, plan.getLastOperator());
    extend->computeFactorizedSchema();
    plan.setLastOperator(std::move(extend));
    // update cardinality estimation info
    if (extendToNewGroup) {
        auto extensionRate = getExtensionRate(*rel, *boundNode, direction);
        plan.getSchema()
            ->getGroup(nbrNode->getInternalIDPropertyName())
            ->setMultiplier(extensionRate);
    }
    plan.increaseCost(plan.getCardinality());
}

void JoinOrderEnumerator::planJoin(const expression_vector& joinNodeIDs, JoinType joinType,
    std::shared_ptr<Expression> mark, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    switch (joinType) {
    case JoinType::INNER:
    case JoinType::LEFT: {
        assert(mark == nullptr);
        appendHashJoin(joinNodeIDs, joinType, probePlan, buildPlan);
    } break;
    case JoinType::MARK: {
        assert(mark != nullptr);
        appendMarkJoin(joinNodeIDs, mark, probePlan, buildPlan);
    } break;
    default:
        assert(false);
    }
}

void JoinOrderEnumerator::appendHashJoin(const expression_vector& joinNodeIDs, JoinType joinType,
    LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto hashJoin = make_shared<LogicalHashJoin>(
        joinNodeIDs, joinType, probePlan.getLastOperator(), buildPlan.getLastOperator());
    // Apply flattening to probe side
    auto groupsPosToFlattenOnProbeSide = hashJoin->getGroupsPosToFlattenOnProbeSide();
    QueryPlanner::appendFlattens(groupsPosToFlattenOnProbeSide, probePlan);
    hashJoin->setChild(0, probePlan.getLastOperator());
    // Apply flattening to build side
    QueryPlanner::appendFlattens(hashJoin->getGroupsPosToFlattenOnBuildSide(), buildPlan);
    hashJoin->setChild(1, buildPlan.getLastOperator());
    hashJoin->computeFactorizedSchema();
    probePlan.increaseCost(probePlan.getCardinality() + buildPlan.getCardinality());
    if (!groupsPosToFlattenOnProbeSide.empty()) {
        probePlan.multiplyCardinality(
            buildPlan.getCardinality() * EnumeratorKnobs::PREDICATE_SELECTIVITY);
        probePlan.multiplyCost(EnumeratorKnobs::FLAT_PROBE_PENALTY);
    }
    probePlan.setLastOperator(std::move(hashJoin));
}

void JoinOrderEnumerator::appendMarkJoin(const expression_vector& joinNodeIDs,
    const std::shared_ptr<Expression>& mark, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto hashJoin = make_shared<LogicalHashJoin>(
        joinNodeIDs, mark, probePlan.getLastOperator(), buildPlan.getLastOperator());
    // Apply flattening to probe side
    QueryPlanner::appendFlattens(hashJoin->getGroupsPosToFlattenOnProbeSide(), probePlan);
    hashJoin->setChild(0, probePlan.getLastOperator());
    // Apply flattening to build side
    QueryPlanner::appendFlattens(hashJoin->getGroupsPosToFlattenOnBuildSide(), buildPlan);
    hashJoin->setChild(1, buildPlan.getLastOperator());
    hashJoin->computeFactorizedSchema();
    probePlan.increaseCost(probePlan.getCardinality() + buildPlan.getCardinality());
    probePlan.setLastOperator(std::move(hashJoin));
}

void JoinOrderEnumerator::appendIntersect(const std::shared_ptr<Expression>& intersectNodeID,
    binder::expression_vector& boundNodeIDs, LogicalPlan& probePlan,
    std::vector<std::unique_ptr<LogicalPlan>>& buildPlans) {
    assert(boundNodeIDs.size() == buildPlans.size());
    std::vector<std::shared_ptr<LogicalOperator>> buildChildren;
    binder::expression_vector keyNodeIDs;
    for (auto i = 0u; i < buildPlans.size(); ++i) {
        keyNodeIDs.push_back(boundNodeIDs[i]);
        buildChildren.push_back(buildPlans[i]->getLastOperator());
    }
    auto intersect = make_shared<LogicalIntersect>(intersectNodeID, std::move(keyNodeIDs),
        probePlan.getLastOperator(), std::move(buildChildren));
    QueryPlanner::appendFlattens(intersect->getGroupsPosToFlattenOnProbeSide(), probePlan);
    intersect->setChild(0, probePlan.getLastOperator());
    for (auto i = 0u; i < buildPlans.size(); ++i) {
        QueryPlanner::appendFlattens(
            intersect->getGroupsPosToFlattenOnBuildSide(i), *buildPlans[i]);
        intersect->setChild(i + 1, buildPlans[i]->getLastOperator());
    }
    intersect->computeFactorizedSchema();
    probePlan.setLastOperator(std::move(intersect));
}

void JoinOrderEnumerator::appendCrossProduct(LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto crossProduct =
        make_shared<LogicalCrossProduct>(probePlan.getLastOperator(), buildPlan.getLastOperator());
    crossProduct->computeFactorizedSchema();
    probePlan.increaseCost(probePlan.getCardinality() + buildPlan.getCardinality());
    probePlan.setLastOperator(std::move(crossProduct));
}

expression_vector JoinOrderEnumerator::getPropertiesForVariable(
    Expression& expression, Expression& variable) {
    expression_vector result;
    std::unordered_set<std::string> matchedPropertyNames; // remove duplication
    for (auto& expr : expression.getSubPropertyExpressions()) {
        auto propertyExpression = (PropertyExpression*)expr.get();
        if (propertyExpression->getVariableName() != variable.getUniqueName()) {
            continue;
        }
        if (matchedPropertyNames.contains(propertyExpression->getUniqueName())) {
            continue;
        }
        matchedPropertyNames.insert(propertyExpression->getUniqueName());
        result.push_back(expr);
    }
    return result;
}

uint64_t JoinOrderEnumerator::getExtensionRate(
    const RelExpression& rel, const NodeExpression& boundNode, RelDirection direction) {
    double numBoundNodes = 0;
    double numRels = 0;
    for (auto boundNodeTableID : boundNode.getTableIDs()) {
        numBoundNodes +=
            nodesStatistics.getNodeStatisticsAndDeletedIDs(boundNodeTableID)->getNumTuples();
        for (auto relTableID : rel.getTableIDs()) {
            auto relStatistic = (storage::RelStatistics*)relsStatistics.getReadOnlyVersion()
                                    ->tableStatisticPerTable[relTableID]
                                    .get();
            numRels += relStatistic->getNumTuples();
        }
    }
    return ceil(numRels / numBoundNodes);
}

expression_vector JoinOrderEnumerator::getNewlyMatchedExpressions(
    const std::vector<SubqueryGraph>& prevSubgraphs, const SubqueryGraph& newSubgraph,
    const expression_vector& expressions) {
    expression_vector result;
    for (auto& expression : expressions) {
        if (isExpressionNewlyMatched(prevSubgraphs, newSubgraph, *expression)) {
            result.push_back(expression);
        }
    }
    return result;
}

bool JoinOrderEnumerator::isExpressionNewlyMatched(const std::vector<SubqueryGraph>& prevSubgraphs,
    const SubqueryGraph& newSubgraph, Expression& expression) {
    auto variables = expression.getDependentVariableNames();
    for (auto& prevSubgraph : prevSubgraphs) {
        if (prevSubgraph.containAllVariables(variables)) {
            return false; // matched in prev subgraph
        }
    }
    return newSubgraph.containAllVariables(variables);
}

} // namespace planner
} // namespace kuzu
