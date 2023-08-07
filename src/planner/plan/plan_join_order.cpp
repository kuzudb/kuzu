#include "binder/expression_visitor.h"
#include "planner/join_order/cost_model.h"
#include "planner/logical_plan/scan/logical_scan_node.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> QueryPlanner::planQueryGraphCollection(
    const QueryGraphCollection& queryGraphCollection, const expression_vector& predicates) {
    return getBestPlan(enumerateQueryGraphCollection(queryGraphCollection, predicates));
}

std::unique_ptr<LogicalPlan> QueryPlanner::planQueryGraphCollectionInNewContext(
    const expression_vector& expressionsToExcludeScan,
    const QueryGraphCollection& queryGraphCollection, const expression_vector& predicates) {
    auto prevContext = enterContext(expressionsToExcludeScan);
    auto plans = enumerateQueryGraphCollection(queryGraphCollection, predicates);
    exitContext(std::move(prevContext));
    return getBestPlan(std::move(plans));
}

std::vector<std::unique_ptr<LogicalPlan>> QueryPlanner::enumerateQueryGraphCollection(
    const QueryGraphCollection& queryGraphCollection, const expression_vector& predicates) {
    assert(queryGraphCollection.getNumQueryGraphs() > 0);
    // project predicates on each query graph
    std::vector<expression_vector> predicatesToPushDownPerGraph;
    predicatesToPushDownPerGraph.resize(queryGraphCollection.getNumQueryGraphs());
    expression_vector predicatesToPullUp;
    for (auto& predicate : predicates) {
        bool needToPullUp = true;
        for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
            auto queryGraph = queryGraphCollection.getQueryGraph(i);
            if (queryGraph->canProjectExpression(predicate)) {
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
        plansPerQueryGraph.push_back(enumerateQueryGraph(
            queryGraphCollection.getQueryGraph(i), predicatesToPushDownPerGraph[i]));
    }
    // take cross products
    auto result = std::move(plansPerQueryGraph[0]);
    for (auto i = 1u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
        result = planCrossProduct(std::move(result), std::move(plansPerQueryGraph[i]));
    }
    // apply remaining predicates
    for (auto& plan : result) {
        for (auto& predicate : predicatesToPullUp) {
            appendFilter(predicate, *plan);
        }
    }
    return result;
}

std::vector<std::unique_ptr<LogicalPlan>> QueryPlanner::enumerateQueryGraph(
    QueryGraph* queryGraph, expression_vector& predicates) {
    context->init(queryGraph, predicates);
    cardinalityEstimator->initNodeIDDom(queryGraph);
    planBaseTableScan();
    context->currentLevel++;
    while (context->currentLevel < context->maxLevel) {
        planLevel(context->currentLevel++);
    }
    return std::move(context->getPlans(context->getFullyMatchedSubqueryGraph()));
}

void QueryPlanner::planLevel(uint32_t level) {
    assert(level > 1);
    if (level > MAX_LEVEL_TO_PLAN_EXACTLY) {
        planLevelApproximately(level);
    } else {
        planLevelExactly(level);
    }
}

void QueryPlanner::planLevelExactly(uint32_t level) {
    auto maxLeftLevel = floor(level / 2.0);
    for (auto leftLevel = 1u; leftLevel <= maxLeftLevel; ++leftLevel) {
        auto rightLevel = level - leftLevel;
        if (leftLevel > 1) { // wcoj requires at least 2 rels
            planWCOJoin(leftLevel, rightLevel);
        }
        planInnerJoin(leftLevel, rightLevel);
    }
}

void QueryPlanner::planLevelApproximately(uint32_t level) {
    planInnerJoin(1, level - 1);
}

static bool isExpressionNewlyMatched(const std::vector<SubqueryGraph>& prevSubgraphs,
    const SubqueryGraph& newSubgraph, const std::shared_ptr<Expression>& expression) {
    auto expressionCollector = std::make_unique<ExpressionCollector>();
    auto variables = expressionCollector->getDependentVariableNames(expression);
    for (auto& prevSubgraph : prevSubgraphs) {
        if (prevSubgraph.containAllVariables(variables)) {
            return false; // matched in prev subgraph
        }
    }
    return newSubgraph.containAllVariables(variables);
}

static expression_vector getNewlyMatchedExpressions(const std::vector<SubqueryGraph>& prevSubgraphs,
    const SubqueryGraph& newSubgraph, const expression_vector& expressions) {
    expression_vector result;
    for (auto& expression : expressions) {
        if (isExpressionNewlyMatched(prevSubgraphs, newSubgraph, expression)) {
            result.push_back(expression);
        }
    }
    return result;
}

static binder::expression_vector getNewlyMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const binder::expression_vector& expressions) {
    return getNewlyMatchedExpressions(
        std::vector<SubqueryGraph>{prevSubgraph}, newSubgraph, expressions);
}

void QueryPlanner::planBaseTableScan() {
    auto queryGraph = context->getQueryGraph();
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes(); ++nodePos) {
        planNodeScan(nodePos);
    }
    for (auto relPos = 0u; relPos < queryGraph->getNumQueryRels(); ++relPos) {
        planRelScan(relPos);
    }
}

void QueryPlanner::planNodeScan(uint32_t nodePos) {
    auto node = context->queryGraph->getQueryNode(nodePos);
    auto newSubgraph = context->getEmptySubqueryGraph();
    newSubgraph.addQueryNode(nodePos);
    auto plan = std::make_unique<LogicalPlan>();
    // In un-nested subquery, e.g. MATCH (a) OPTIONAL MATCH (a)-[e1]->(b), the inner query
    // ("(a)-[e1]->(b)") needs to scan a, which is already scanned in the outer query (a). To avoid
    // scanning storage twice, we keep track of node table "a" and make sure when planning inner
    // query, we only scan internal ID of "a".
    if (!context->nodeToScanFromInnerAndOuter(node.get())) {
        appendScanNodeID(node, *plan);
        auto properties = getProperties(*node);
        appendScanNodeProperties(properties, node, *plan);
        auto predicates = getNewlyMatchedExpressions(
            context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
        appendFilters(predicates, *plan);
    } else {
        appendScanNodeID(node, *plan);
    }
    context->addPlan(newSubgraph, std::move(plan));
}

static std::pair<std::shared_ptr<NodeExpression>, std::shared_ptr<NodeExpression>>
getBoundAndNbrNodes(const RelExpression& rel, ExtendDirection direction) {
    assert(direction != ExtendDirection::BOTH);
    auto boundNode = direction == ExtendDirection::FWD ? rel.getSrcNode() : rel.getDstNode();
    auto dstNode = direction == ExtendDirection::FWD ? rel.getDstNode() : rel.getSrcNode();
    return make_pair(boundNode, dstNode);
}

void QueryPlanner::planRelScan(uint32_t relPos) {
    auto rel = context->queryGraph->getQueryRel(relPos);
    auto newSubgraph = context->getEmptySubqueryGraph();
    newSubgraph.addQueryRel(relPos);
    auto predicates = getNewlyMatchedExpressions(
        context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
    // Regardless of whether rel is directed or not,
    // we always enumerate two plans, one from src to dst, and the other from dst to src.
    for (auto direction : {ExtendDirection::FWD, ExtendDirection::BWD}) {
        auto plan = std::make_unique<LogicalPlan>();
        auto [boundNode, nbrNode] = getBoundAndNbrNodes(*rel, direction);
        auto extendDirection = ExtendDirectionUtils::getExtendDirection(*rel, *boundNode);
        appendScanNodeID(boundNode, *plan);
        appendExtendAndFilter(boundNode, nbrNode, rel, extendDirection, predicates, *plan);
        context->addPlan(newSubgraph, std::move(plan));
    }
}

void QueryPlanner::appendExtendAndFilter(std::shared_ptr<NodeExpression> boundNode,
    std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
    ExtendDirection direction, const expression_vector& predicates, LogicalPlan& plan) {
    switch (rel->getRelType()) {
    case common::QueryRelType::NON_RECURSIVE: {
        auto properties = getProperties(*rel);
        appendNonRecursiveExtend(boundNode, nbrNode, rel, direction, properties, plan);
    } break;
    case common::QueryRelType::VARIABLE_LENGTH:
    case common::QueryRelType::SHORTEST:
    case common::QueryRelType::ALL_SHORTEST: {
        appendRecursiveExtend(boundNode, nbrNode, rel, direction, plan);
    } break;
    default:
        throw common::NotImplementedException("JoinOrderEnumerator::appendExtendAndFilter");
    }
    appendFilters(predicates, plan);
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

void QueryPlanner::planWCOJoin(uint32_t leftLevel, uint32_t rightLevel) {
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

static LogicalScanNode* getSequentialScanNodeOperator(LogicalOperator* op) {
    switch (op->getOperatorType()) {
    case LogicalOperatorType::FLATTEN:
    case LogicalOperatorType::FILTER:
    case LogicalOperatorType::SCAN_NODE_PROPERTY:
    case LogicalOperatorType::EXTEND:
    case LogicalOperatorType::PROJECTION: { // operators we directly search through
        return getSequentialScanNodeOperator(op->getChild(0).get());
    }
    case LogicalOperatorType::SCAN_NODE: {
        return (LogicalScanNode*)op;
    }
    default:
        return nullptr;
    }
}

// Check whether given node ID has sequential guarantee on the plan.
static bool isNodeSequentialOnPlan(LogicalPlan& plan, const NodeExpression& node) {
    auto sequentialScanNode = getSequentialScanNodeOperator(plan.getLastOperator().get());
    if (sequentialScanNode == nullptr) {
        return false;
    }
    return sequentialScanNode->getNode()->getUniqueName() == node.getUniqueName();
}

// As a heuristic for wcoj, we always pick rel scan that starts from the bound node.
static std::unique_ptr<LogicalPlan> getWCOJBuildPlanForRel(
    std::vector<std::unique_ptr<LogicalPlan>>& candidatePlans, const NodeExpression& boundNode) {
    std::unique_ptr<LogicalPlan> result;
    for (auto& candidatePlan : candidatePlans) {
        if (isNodeSequentialOnPlan(*candidatePlan, boundNode)) {
            assert(result == nullptr);
            result = candidatePlan->shallowCopy();
        }
    }
    return result;
}

void QueryPlanner::planWCOJoin(const SubqueryGraph& subgraph,
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
        auto relPlan = getWCOJBuildPlanForRel(relPlanCandidates, *boundNode);
        if (relPlan == nullptr) { // Cannot find a suitable rel plan.
            return;
        }
        relPlans.push_back(std::move(relPlan));
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
            appendFilter(predicate, *leftPlanCopy);
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

void QueryPlanner::planInnerJoin(uint32_t leftLevel, uint32_t rightLevel) {
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
            if (tryPlanINLJoin(rightSubgraph, nbrSubgraph, joinNodes)) {
                continue;
            }
            planInnerHashJoin(rightSubgraph, nbrSubgraph, joinNodes, leftLevel != rightLevel);
        }
    }
}

bool QueryPlanner::tryPlanINLJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
    const std::vector<std::shared_ptr<NodeExpression>>& joinNodes) {
    if (joinNodes.size() > 1) {
        return false;
    }
    if (!subgraph.isSingleRel() && !otherSubgraph.isSingleRel()) {
        return false;
    }
    if (subgraph.isSingleRel()) { // Always put single rel subgraph to right.
        return tryPlanINLJoin(otherSubgraph, subgraph, joinNodes);
    }
    auto relPos = UINT32_MAX;
    for (auto i = 0u; i < context->queryGraph->getNumQueryRels(); ++i) {
        if (otherSubgraph.queryRelsSelector[i]) {
            relPos = i;
        }
    }
    assert(relPos != UINT32_MAX);
    auto rel = context->queryGraph->getQueryRel(relPos);
    auto boundNode = joinNodes[0];
    auto nbrNode =
        boundNode->getUniqueName() == rel->getSrcNodeName() ? rel->getDstNode() : rel->getSrcNode();
    auto extendDirection = ExtendDirectionUtils::getExtendDirection(*rel, *boundNode);
    auto newSubgraph = subgraph;
    newSubgraph.addQueryRel(relPos);
    auto predicates =
        getNewlyMatchedExpressions(subgraph, newSubgraph, context->getWhereExpressions());
    bool hasAppliedINLJoin = false;
    for (auto& prevPlan : context->getPlans(subgraph)) {
        if (isNodeSequentialOnPlan(*prevPlan, *boundNode)) {
            auto plan = prevPlan->shallowCopy();
            appendExtendAndFilter(boundNode, nbrNode, rel, extendDirection, predicates, *plan);
            context->addPlan(newSubgraph, std::move(plan));
            hasAppliedINLJoin = true;
        }
    }
    return hasAppliedINLJoin;
}

void QueryPlanner::planInnerHashJoin(const SubqueryGraph& subgraph,
    const SubqueryGraph& otherSubgraph, std::vector<std::shared_ptr<NodeExpression>> joinNodes,
    bool flipPlan) {
    auto newSubgraph = subgraph;
    newSubgraph.addSubqueryGraph(otherSubgraph);
    auto maxCost = context->subPlansTable->getMaxCost(newSubgraph);
    binder::expression_vector joinNodeIDs;
    for (auto& joinNode : joinNodes) {
        joinNodeIDs.push_back(joinNode->getInternalIDProperty());
    }
    auto predicates =
        getNewlyMatchedExpressions(std::vector<SubqueryGraph>{subgraph, otherSubgraph}, newSubgraph,
            context->getWhereExpressions());
    for (auto& leftPlan : context->getPlans(subgraph)) {
        for (auto& rightPlan : context->getPlans(otherSubgraph)) {
            if (CostModel::computeHashJoinCost(joinNodeIDs, *leftPlan, *rightPlan) < maxCost) {
                auto leftPlanProbeCopy = leftPlan->shallowCopy();
                auto rightPlanBuildCopy = rightPlan->shallowCopy();
                appendHashJoin(
                    joinNodeIDs, common::JoinType::INNER, *leftPlanProbeCopy, *rightPlanBuildCopy);
                appendFilters(predicates, *leftPlanProbeCopy);
                context->addPlan(newSubgraph, std::move(leftPlanProbeCopy));
            }
            // flip build and probe side to get another HashJoin plan
            if (flipPlan &&
                CostModel::computeHashJoinCost(joinNodeIDs, *rightPlan, *leftPlan) < maxCost) {
                auto leftPlanBuildCopy = leftPlan->shallowCopy();
                auto rightPlanProbeCopy = rightPlan->shallowCopy();
                appendHashJoin(
                    joinNodeIDs, common::JoinType::INNER, *rightPlanProbeCopy, *leftPlanBuildCopy);
                appendFilters(predicates, *rightPlanProbeCopy);
                context->addPlan(newSubgraph, std::move(rightPlanProbeCopy));
            }
        }
    }
}

std::vector<std::unique_ptr<LogicalPlan>> QueryPlanner::planCrossProduct(
    std::vector<std::unique_ptr<LogicalPlan>> leftPlans,
    std::vector<std::unique_ptr<LogicalPlan>> rightPlans) {
    std::vector<std::unique_ptr<LogicalPlan>> result;
    for (auto& leftPlan : leftPlans) {
        for (auto& rightPlan : rightPlans) {
            auto leftPlanCopy = leftPlan->shallowCopy();
            auto rightPlanCopy = rightPlan->shallowCopy();
            appendCrossProduct(common::AccumulateType::REGULAR, *leftPlanCopy, *rightPlanCopy);
            result.push_back(std::move(leftPlanCopy));
        }
    }
    return result;
}

} // namespace planner
} // namespace kuzu
