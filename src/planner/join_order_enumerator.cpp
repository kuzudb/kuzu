#include "planner/join_order_enumerator.h"

#include "planner/asp_optimizer.h"
#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_cross_product.h"
#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "planner/logical_plan/logical_operator/logical_ftable_scan.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "planner/logical_plan/logical_operator/sink_util.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/projection_planner.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

vector<unique_ptr<LogicalPlan>> JoinOrderEnumerator::enumerate(
    const QueryGraphCollection& queryGraphCollection, expression_vector& predicates) {
    assert(queryGraphCollection.getNumQueryGraphs() > 0);
    // project predicates on each query graph
    vector<expression_vector> predicatesToPushDownPerGraph;
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
    vector<vector<unique_ptr<LogicalPlan>>> plansPerQueryGraph;
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

vector<unique_ptr<LogicalPlan>> JoinOrderEnumerator::planCrossProduct(
    vector<unique_ptr<LogicalPlan>> leftPlans, vector<unique_ptr<LogicalPlan>> rightPlans) {
    vector<unique_ptr<LogicalPlan>> result;
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

vector<unique_ptr<LogicalPlan>> JoinOrderEnumerator::enumerate(
    QueryGraph* queryGraph, expression_vector& predicates) {
    context->init(queryGraph, predicates);
    if (!context->expressionsToScanFromOuter.empty()) {
        planOuterExpressionsScan(context->expressionsToScanFromOuter);
    }
    planTableScan();
    context->currentLevel++;
    while (context->currentLevel < context->maxLevel) {
        planLevel(context->currentLevel++);
    }
    return move(context->getPlans(context->getFullyMatchedSubqueryGraph()));
}

unique_ptr<JoinOrderEnumeratorContext> JoinOrderEnumerator::enterSubquery(LogicalPlan* outerPlan,
    expression_vector expressionsToScan, vector<shared_ptr<NodeExpression>> nodesToScanTwice) {
    auto prevContext = std::move(context);
    context = make_unique<JoinOrderEnumeratorContext>();
    context->outerPlan = outerPlan;
    context->expressionsToScanFromOuter = std::move(expressionsToScan);
    context->nodesToScanTwice = std::move(nodesToScanTwice);
    return prevContext;
}

void JoinOrderEnumerator::exitSubquery(unique_ptr<JoinOrderEnumeratorContext> prevContext) {
    context = move(prevContext);
}

void JoinOrderEnumerator::planLevel(uint32_t level) {
    assert(level > 1);
    auto maxLeftLevel = floor(level / 2.0);
    for (auto leftLevel = 1u; leftLevel <= maxLeftLevel; ++leftLevel) {
        auto rightLevel = level - leftLevel;
        if (leftLevel > 1) { // wcoj requires at least 2 rels
            planWCOJoin(leftLevel, rightLevel);
        }
        planInnerJoin(leftLevel, rightLevel);
    }
    context->subPlansTable->finalizeLevel(level);
}

void JoinOrderEnumerator::planOuterExpressionsScan(expression_vector& expressions) {
    auto newSubgraph = context->getEmptySubqueryGraph();
    for (auto& expression : expressions) {
        if (expression->getDataType().typeID == NODE_ID) {
            auto node = static_pointer_cast<NodeExpression>(expression->getChild(0));
            auto nodePos = context->getQueryGraph()->getQueryNodePos(*node);
            newSubgraph.addQueryNode(nodePos);
        }
    }
    auto plan = make_unique<LogicalPlan>();
    appendFTableScan(context->outerPlan, expressions, *plan);
    auto predicates = getNewlyMatchedExpressions(
        context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
    for (auto& predicate : predicates) {
        queryPlanner->appendFilter(predicate, *plan);
    }
    QueryPlanner::appendDistinct(expressions, *plan);
    context->addPlan(newSubgraph, std::move(plan));
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

static bool isPrimaryPropertyAndLiteralPair(const Expression& left, const Expression& right,
    const NodeExpression& node, uint32_t primaryKeyID) {
    if (left.expressionType != PROPERTY || right.expressionType != LITERAL) {
        return false;
    }
    auto propertyExpression = (const PropertyExpression&)left;
    return propertyExpression.getPropertyID(node.getTableID()) == primaryKeyID;
}

static bool isIndexScanExpression(
    Expression& expression, const NodeExpression& node, uint32_t primaryKeyID) {
    if (expression.expressionType != EQUALS) { // check equality comparison
        return false;
    }
    auto left = expression.getChild(0);
    auto right = expression.getChild(1);
    if (isPrimaryPropertyAndLiteralPair(*left, *right, node, primaryKeyID) ||
        isPrimaryPropertyAndLiteralPair(*right, *left, node, primaryKeyID)) {
        return true;
    }
    return false;
}

static shared_ptr<Expression> extractIndexExpression(Expression& expression) {
    if (expression.getChild(0)->expressionType == LITERAL) {
        return expression.getChild(0);
    }
    return expression.getChild(1);
}

static pair<shared_ptr<Expression>, expression_vector> splitIndexAndPredicates(
    const CatalogContent& catalogContent, NodeExpression& node,
    const expression_vector& predicates) {
    auto nodeTableSchema = catalogContent.getNodeTableSchema(node.getTableID());
    auto primaryKeyID = nodeTableSchema->getPrimaryKey().propertyID;
    shared_ptr<Expression> indexExpression;
    expression_vector predicatesToApply;
    for (auto& predicate : predicates) {
        if (isIndexScanExpression(*predicate, node, primaryKeyID)) {
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
    auto plan = make_unique<LogicalPlan>();
    auto predicates = getNewlyMatchedExpressions(
        context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
    // In un-nested subquery, e.g. MATCH (a) OPTIONAL MATCH (a)-[e1]->(b), the inner query
    // ("(a)-[e1]->(b)") needs to scan a, which is already scanned in the outer query (a). To avoid
    // scanning storage twice, we keep track of node table "a" and make sure when planning inner
    // query, we only scan internal ID of "a".
    if (!context->nodeNeedScanTwice(node.get())) {
        shared_ptr<Expression> indexExpression = nullptr;
        expression_vector predicatesToApply = predicates;
        if (node->getNumTableIDs() == 1) { // check for index scan
            auto [_indexExpression, _predicatesToApply] =
                splitIndexAndPredicates(*catalog.getReadOnlyVersion(), *node, predicates);
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
    expression_vector& predicates, shared_ptr<NodeExpression> node, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        auto propertiesToScan = getPropertiesForVariable(*predicate, *node);
        queryPlanner->appendScanNodePropIfNecessary(propertiesToScan, node, plan);
        queryPlanner->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::planPropertyScansForNode(
    shared_ptr<NodeExpression> node, LogicalPlan& plan) {
    auto properties = queryPlanner->getPropertiesForNode(*node);
    queryPlanner->appendScanNodePropIfNecessary(properties, node, plan);
}

void JoinOrderEnumerator::planRelScan(uint32_t relPos) {
    auto rel = context->queryGraph->getQueryRel(relPos);
    auto newSubgraph = context->getEmptySubqueryGraph();
    newSubgraph.addQueryRel(relPos);
    auto predicates = getNewlyMatchedExpressions(
        context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
    for (auto direction : REL_DIRECTIONS) {
        auto plan = make_unique<LogicalPlan>();
        auto boundNode = direction == FWD ? rel->getSrcNode() : rel->getDstNode();
        appendScanNode(boundNode, *plan);
        planRelExtendFiltersAndProperties(rel, direction, predicates, *plan);
        context->addPlan(newSubgraph, move(plan));
    }
}

void JoinOrderEnumerator::planFiltersForRel(
    expression_vector& predicates, RelExpression& rel, RelDirection direction, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        auto relPropertiesToScan = getPropertiesForVariable(*predicate, rel);
        queryPlanner->appendScanRelPropsIfNecessary(relPropertiesToScan, rel, direction, plan);
        queryPlanner->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::planPropertyScansForRel(
    RelExpression& rel, RelDirection direction, LogicalPlan& plan) {
    auto relProperties = queryPlanner->getPropertiesForRel(rel);
    queryPlanner->appendScanRelPropsIfNecessary(relProperties, rel, direction, plan);
}

static unordered_map<uint32_t, vector<shared_ptr<RelExpression>>> populateIntersectRelCandidates(
    const QueryGraph& queryGraph, const SubqueryGraph& subgraph) {
    unordered_map<uint32_t, vector<shared_ptr<RelExpression>>> intersectNodePosToRelsMap;
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
                {intersectNodePos, vector<shared_ptr<RelExpression>>{}});
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
static unique_ptr<LogicalPlan> getWCOJBuildPlanForRel(
    vector<unique_ptr<LogicalPlan>>& candidatePlans, const NodeExpression& boundNode) {
    unique_ptr<LogicalPlan> result;
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
    vector<shared_ptr<RelExpression>> rels, const shared_ptr<NodeExpression>& intersectNode) {
    auto newSubgraph = subgraph;
    vector<SubqueryGraph> prevSubgraphs;
    prevSubgraphs.push_back(subgraph);
    vector<shared_ptr<NodeExpression>> boundNodes;
    vector<unique_ptr<LogicalPlan>> relPlans;
    for (auto& rel : rels) {
        auto boundNode = rel->getSrcNodeName() == intersectNode->getUniqueName() ?
                             rel->getDstNode() :
                             rel->getSrcNode();
        boundNodes.push_back(boundNode);
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
        vector<unique_ptr<LogicalPlan>> rightPlansCopy;
        for (auto& relPlan : relPlans) {
            rightPlansCopy.push_back(relPlan->shallowCopy());
        }
        appendIntersect(intersectNode, boundNodes, *leftPlanCopy, rightPlansCopy);
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
    const SubqueryGraph& otherSubgraph, const vector<shared_ptr<NodeExpression>>& joinNodes) {
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
    const SubqueryGraph& otherSubgraph, const vector<shared_ptr<NodeExpression>>& joinNodes) {
    assert(otherSubgraph.getNumQueryRels() == 1 && joinNodes.size() == 1);
    auto queryGraph = context->getQueryGraph();
    auto relPos = extractJoinRelPos(otherSubgraph, *queryGraph);
    auto rel = queryGraph->getQueryRel(relPos);
    auto newSubgraph = subgraph;
    newSubgraph.addQueryRel(relPos);
    auto predicates =
        getNewlyMatchedExpressions(subgraph, newSubgraph, context->getWhereExpressions());
    for (auto& prevPlan : context->getPlans(subgraph)) {
        if (isNodeSequential(*prevPlan, joinNodes[0].get())) {
            auto plan = prevPlan->shallowCopy();
            auto direction = joinNodes[0]->getUniqueName() == rel->getSrcNodeName() ? FWD : BWD;
            planRelExtendFiltersAndProperties(rel, direction, predicates, *plan);
            context->addPlan(newSubgraph, move(plan));
        }
    }
}

void JoinOrderEnumerator::planInnerHashJoin(const SubqueryGraph& subgraph,
    const SubqueryGraph& otherSubgraph, vector<shared_ptr<NodeExpression>> joinNodes,
    bool flipPlan) {
    auto newSubgraph = subgraph;
    newSubgraph.addSubqueryGraph(otherSubgraph);
    auto predicates = getNewlyMatchedExpressions(vector<SubqueryGraph>{subgraph, otherSubgraph},
        newSubgraph, context->getWhereExpressions());
    for (auto& leftPlan : context->getPlans(subgraph)) {
        for (auto& rightPlan : context->getPlans(otherSubgraph)) {
            auto leftPlanProbeCopy = leftPlan->shallowCopy();
            auto rightPlanBuildCopy = rightPlan->shallowCopy();
            auto leftPlanBuildCopy = leftPlan->shallowCopy();
            auto rightPlanProbeCopy = rightPlan->shallowCopy();
            planInnerHashJoin(joinNodes, *leftPlanProbeCopy, *rightPlanBuildCopy);
            planFiltersForHashJoin(predicates, *leftPlanProbeCopy);
            context->addPlan(newSubgraph, move(leftPlanProbeCopy));
            // flip build and probe side to get another HashJoin plan
            if (flipPlan) {
                planInnerHashJoin(joinNodes, *rightPlanProbeCopy, *leftPlanBuildCopy);
                planFiltersForHashJoin(predicates, *rightPlanProbeCopy);
                context->addPlan(newSubgraph, move(rightPlanProbeCopy));
            }
        }
    }
}

void JoinOrderEnumerator::planFiltersForHashJoin(expression_vector& predicates, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        queryPlanner->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::appendFTableScan(
    LogicalPlan* outerPlan, expression_vector& expressionsToScan, LogicalPlan& plan) {
    unordered_map<uint32_t, expression_vector> groupPosToExpressionsMap;
    for (auto& expression : expressionsToScan) {
        auto outerPos = outerPlan->getSchema()->getGroupPos(expression->getUniqueName());
        if (!groupPosToExpressionsMap.contains(outerPos)) {
            groupPosToExpressionsMap.insert({outerPos, expression_vector{}});
        }
        groupPosToExpressionsMap.at(outerPos).push_back(expression);
    }
    vector<uint64_t> flatOutputGroupPositions;
    auto schema = plan.getSchema();
    for (auto& [outerPos, expressions] : groupPosToExpressionsMap) {
        auto innerPos = schema->createGroup();
        schema->insertToGroupAndScope(expressions, innerPos);
        if (outerPlan->getSchema()->getGroup(outerPos)->getIsFlat()) {
            schema->flattenGroup(innerPos);
            flatOutputGroupPositions.push_back(innerPos);
        }
    }
    assert(outerPlan->getLastOperator()->getLogicalOperatorType() ==
           LogicalOperatorType::LOGICAL_ACCUMULATE);
    auto logicalAcc = (LogicalAccumulate*)outerPlan->getLastOperator().get();
    auto fTableScan = make_shared<LogicalFTableScan>(
        expressionsToScan, logicalAcc->getExpressions(), flatOutputGroupPositions);
    plan.setLastOperator(std::move(fTableScan));
}

void JoinOrderEnumerator::appendScanNode(shared_ptr<NodeExpression>& node, LogicalPlan& plan) {
    assert(plan.isEmpty());
    auto schema = plan.getSchema();
    auto scan = make_shared<LogicalScanNode>(node);
    scan->computeSchema(*schema);
    // update cardinality
    auto group = schema->getGroup(node->getInternalIDPropertyName());
    auto numNodes = 0u;
    for (auto& tableID : node->getTableIDs()) {
        numNodes += nodesStatistics.getNodeStatisticsAndDeletedIDs(tableID)->getNumTuples();
    }
    group->setMultiplier(numNodes);
    plan.setLastOperator(std::move(scan));
}

void JoinOrderEnumerator::appendIndexScanNode(
    shared_ptr<NodeExpression>& node, shared_ptr<Expression> indexExpression, LogicalPlan& plan) {
    assert(plan.isEmpty());
    auto schema = plan.getSchema();
    auto scan = make_shared<LogicalIndexScanNode>(node, std::move(indexExpression));
    scan->computeSchema(*schema);
    // update cardinality
    auto group = schema->getGroup(node->getInternalIDPropertyName());
    group->setMultiplier(1);
    plan.setLastOperator(std::move(scan));
}

void JoinOrderEnumerator::appendExtend(
    shared_ptr<RelExpression>& rel, RelDirection direction, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto boundNode = FWD == direction ? rel->getSrcNode() : rel->getDstNode();
    auto nbrNode = FWD == direction ? rel->getDstNode() : rel->getSrcNode();
    auto isColumn =
        catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(rel->getTableID(), direction);
    if (rel->isVariableLength() || !isColumn) {
        QueryPlanner::appendFlattenIfNecessary(boundNode->getInternalIDProperty(), plan);
    }
    auto extend = make_shared<LogicalExtend>(boundNode, nbrNode, rel->getTableID(), direction,
        isColumn, rel->getLowerBound(), rel->getUpperBound(), plan.getLastOperator());
    extend->computeSchema(*schema);
    plan.setLastOperator(move(extend));
    // update cardinality estimation info
    if (!isColumn) {
        auto extensionRate =
            getExtensionRate(boundNode->getTableID(), rel->getTableID(), direction);
        schema->getGroup(nbrNode->getInternalIDPropertyName())->setMultiplier(extensionRate);
    }
    plan.increaseCost(plan.getCardinality());
}

void JoinOrderEnumerator::planJoin(const vector<shared_ptr<NodeExpression>>& joinNodes,
    JoinType joinType, shared_ptr<Expression> mark, LogicalPlan& probePlan,
    LogicalPlan& buildPlan) {
    auto isProbeAcc = false;
    // TODO(Guodong): Fix asp for multiple join nodes.
    if (ASPOptimizer::canApplyASP(joinNodes, isProbeAcc, probePlan, buildPlan)) {
        ASPOptimizer::applyASP(joinNodes[0], probePlan, buildPlan);
        isProbeAcc = true;
    }
    switch (joinType) {
    case JoinType::INNER:
    case JoinType::LEFT: {
        assert(mark == nullptr);
        appendHashJoin(joinNodes, joinType, isProbeAcc, probePlan, buildPlan);
    } break;
    case JoinType::MARK: {
        assert(mark != nullptr);
        appendMarkJoin(joinNodes, mark, isProbeAcc, probePlan, buildPlan);
    } break;
    default:
        assert(false);
    }
}

static bool isJoinKeyUniqueOnBuildSide(const string& joinNodeID, LogicalPlan& buildPlan) {
    auto buildSchema = buildPlan.getSchema();
    auto numGroupsInScope = buildSchema->getGroupsPosInScope().size();
    bool hasProjectedOutGroups = buildSchema->getNumGroups() > numGroupsInScope;
    if (numGroupsInScope > 1 || hasProjectedOutGroups) {
        return false;
    }
    // Now there is a single factorization group, we need to further make sure joinNodeID comes from
    // ScanNodeID operator. Because if joinNodeID comes from a ColExtend we cannot guarantee the
    // reverse mapping is still many-to-one. We look for the most simple pattern where build plan is
    // linear.
    auto firstop = buildPlan.getLastOperator().get();
    while (firstop->getNumChildren() != 0) {
        if (firstop->getNumChildren() > 1) {
            return false;
        }
        firstop = firstop->getChild(0).get();
    }
    if (firstop->getLogicalOperatorType() != LOGICAL_SCAN_NODE) {
        return false;
    }
    auto scanNodeID = (LogicalScanNode*)firstop;
    if (scanNodeID->getNode()->getInternalIDPropertyName() != joinNodeID) {
        return false;
    }
    return true;
}

void JoinOrderEnumerator::appendHashJoin(const vector<shared_ptr<NodeExpression>>& joinNodes,
    JoinType joinType, bool isProbeAcc, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto& buildSideSchema = *buildPlan.getSchema();
    auto probeSideSchema = probePlan.getSchema();
    probePlan.increaseCost(probePlan.getCardinality() + buildPlan.getCardinality());
    // Flat probe side key group in either of the following two cases:
    // 1. there are multiple join nodes;
    // 2. if the build side contains more than one group or the build side has projected out data
    // chunks, which may increase the multiplicity of data chunks in the build side. The key is to
    // keep probe side key unflat only when we know that there is only 0 or 1 match for each key.
    // TODO(Guodong): when the build side has only flat payloads, we should consider getting rid of
    // flattening probe key, instead duplicating keys as in vectorized processing if necessary.
    if (joinNodes.size() > 1 ||
        !isJoinKeyUniqueOnBuildSide(joinNodes[0]->getInternalIDPropertyName(), buildPlan)) {
        for (auto& joinNode : joinNodes) {
            auto probeSideKeyGroupPos =
                probeSideSchema->getGroupPos(joinNode->getInternalIDPropertyName());
            QueryPlanner::appendFlattenIfNecessary(probeSideKeyGroupPos, probePlan);
        }
        probePlan.multiplyCardinality(
            buildPlan.getCardinality() * EnumeratorKnobs::PREDICATE_SELECTIVITY);
        probePlan.multiplyCost(EnumeratorKnobs::FLAT_PROBE_PENALTY);
    }
    // Flat all but one build side key groups.
    unordered_set<uint32_t> joinNodesGroupPos;
    for (auto& joinNode : joinNodes) {
        joinNodesGroupPos.insert(
            buildSideSchema.getGroupPos(joinNode->getInternalIDPropertyName()));
    }
    QueryPlanner::appendFlattensButOne(joinNodesGroupPos, buildPlan);

    auto numGroupsBeforeMerging = probeSideSchema->getNumGroups();
    vector<string> keys;
    for (auto& joinNode : joinNodes) {
        keys.push_back(joinNode->getInternalIDPropertyName());
    }
    SinkOperatorUtil::mergeSchema(buildSideSchema, *probeSideSchema, keys);
    vector<uint64_t> flatOutputGroupPositions;
    for (auto i = numGroupsBeforeMerging; i < probeSideSchema->getNumGroups(); ++i) {
        if (probeSideSchema->getGroup(i)->getIsFlat()) {
            flatOutputGroupPositions.push_back(i);
        }
    }
    auto hashJoin = make_shared<LogicalHashJoin>(joinNodes, joinType, isProbeAcc,
        buildSideSchema.copy(), flatOutputGroupPositions, buildSideSchema.getExpressionsInScope(),
        probePlan.getLastOperator(), buildPlan.getLastOperator());
    probePlan.setLastOperator(move(hashJoin));
}

void JoinOrderEnumerator::appendMarkJoin(const vector<shared_ptr<NodeExpression>>& joinNodes,
    const shared_ptr<Expression>& mark, bool isProbeAcc, LogicalPlan& probePlan,
    LogicalPlan& buildPlan) {
    auto buildSchema = buildPlan.getSchema();
    auto probeSchema = probePlan.getSchema();
    // Apply flattening all but one on join nodes of both probe and build side.
    unordered_set<uint32_t> joinNodeGroupsPosInProbeSide, joinNodeGroupsPosInBuildSide;
    for (auto& joinNode : joinNodes) {
        joinNodeGroupsPosInProbeSide.insert(
            probeSchema->getGroupPos(joinNode->getInternalIDPropertyName()));
        joinNodeGroupsPosInBuildSide.insert(
            buildSchema->getGroupPos(joinNode->getInternalIDPropertyName()));
    }
    auto markGroupPos = QueryPlanner::appendFlattensButOne(joinNodeGroupsPosInProbeSide, probePlan);
    QueryPlanner::appendFlattensButOne(joinNodeGroupsPosInBuildSide, buildPlan);
    probePlan.increaseCost(probePlan.getCardinality() + buildPlan.getCardinality());
    probeSchema->insertToGroupAndScope(mark, markGroupPos);
    auto hashJoin = make_shared<LogicalHashJoin>(joinNodes, mark, isProbeAcc, buildSchema->copy(),
        probePlan.getLastOperator(), buildPlan.getLastOperator());
    probePlan.setLastOperator(std::move(hashJoin));
}

void JoinOrderEnumerator::appendIntersect(const shared_ptr<NodeExpression>& intersectNode,
    vector<shared_ptr<NodeExpression>>& boundNodes, LogicalPlan& probePlan,
    vector<unique_ptr<LogicalPlan>>& buildPlans) {
    auto intersectNodeID = intersectNode->getInternalIDPropertyName();
    auto probeSchema = probePlan.getSchema();
    assert(boundNodes.size() == buildPlans.size());
    // Write intersect node and rels into a new group regardless of whether rel is n-n.
    auto outGroupPos = probeSchema->createGroup();
    // Write intersect node into output group.
    probeSchema->insertToGroupAndScope(intersectNode->getInternalIDProperty(), outGroupPos);
    vector<shared_ptr<LogicalOperator>> buildChildren;
    vector<unique_ptr<LogicalIntersectBuildInfo>> buildInfos;
    for (auto i = 0u; i < buildPlans.size(); ++i) {
        auto boundNode = boundNodes[i];
        QueryPlanner::appendFlattenIfNecessary(
            probeSchema->getGroupPos(boundNode->getInternalIDPropertyName()), probePlan);
        auto buildPlan = buildPlans[i].get();
        auto buildSchema = buildPlan->getSchema();
        QueryPlanner::appendFlattenIfNecessary(
            buildSchema->getGroupPos(boundNode->getInternalIDPropertyName()), *buildPlan);
        auto expressions = buildSchema->getExpressionsInScope();
        // Write rel properties into output group.
        for (auto& expression : expressions) {
            if (expression->getUniqueName() == intersectNodeID ||
                expression->getUniqueName() == boundNode->getInternalIDPropertyName()) {
                continue;
            }
            probeSchema->insertToGroupAndScope(expression, outGroupPos);
        }
        auto buildInfo =
            make_unique<LogicalIntersectBuildInfo>(boundNode, buildSchema->copy(), expressions);
        buildChildren.push_back(buildPlan->getLastOperator());
        buildInfos.push_back(std::move(buildInfo));
    }
    auto logicalIntersect = make_shared<LogicalIntersect>(intersectNode,
        probePlan.getLastOperator(), std::move(buildChildren), std::move(buildInfos));
    probePlan.setLastOperator(std::move(logicalIntersect));
}

void JoinOrderEnumerator::appendCrossProduct(LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto probeSideSchema = probePlan.getSchema();
    auto buildSideSchema = buildPlan.getSchema();
    probePlan.increaseCost(probePlan.getCardinality() + buildPlan.getCardinality());
    auto numGroupsBeforeMerging = probeSideSchema->getNumGroups();
    SinkOperatorUtil::mergeSchema(*buildSideSchema, *probeSideSchema);
    vector<uint64_t> flatOutputGroupPositions;
    for (auto i = numGroupsBeforeMerging; i < probeSideSchema->getNumGroups(); ++i) {
        if (probeSideSchema->getGroup(i)->getIsFlat()) {
            flatOutputGroupPositions.push_back(i);
        }
    }
    auto crossProduct = make_shared<LogicalCrossProduct>(buildSideSchema->copy(),
        flatOutputGroupPositions, probePlan.getLastOperator(), buildPlan.getLastOperator());
    probePlan.setLastOperator(std::move(crossProduct));
}

expression_vector JoinOrderEnumerator::getPropertiesForVariable(
    Expression& expression, Expression& variable) {
    expression_vector result;
    for (auto& propertyExpression : expression.getSubPropertyExpressions()) {
        if (propertyExpression->getChild(0)->getUniqueName() != variable.getUniqueName()) {
            continue;
        }
        result.push_back(propertyExpression);
    }
    return result;
}

uint64_t JoinOrderEnumerator::getExtensionRate(
    table_id_t boundTableID, table_id_t relTableID, RelDirection relDirection) {
    auto numRels = ((RelStatistics*)relsStatistics.getReadOnlyVersion()
                        ->tableStatisticPerTable[relTableID]
                        .get())
                       ->getNumRelsForDirectionBoundTable(relDirection, boundTableID);
    return ceil(
        (double)numRels /
            nodesStatistics.getNodeStatisticsAndDeletedIDs(boundTableID)->getMaxNodeOffset() +
        1);
}

expression_vector JoinOrderEnumerator::getNewlyMatchedExpressions(
    const vector<SubqueryGraph>& prevSubgraphs, const SubqueryGraph& newSubgraph,
    const expression_vector& expressions) {
    expression_vector result;
    for (auto& expression : expressions) {
        if (isExpressionNewlyMatched(prevSubgraphs, newSubgraph, *expression)) {
            result.push_back(expression);
        }
    }
    return result;
}

bool JoinOrderEnumerator::isExpressionNewlyMatched(const vector<SubqueryGraph>& prevSubgraphs,
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
