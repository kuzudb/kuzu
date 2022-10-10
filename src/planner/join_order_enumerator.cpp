#include "include/join_order_enumerator.h"

#include "include/asp_optimizer.h"
#include "include/enumerator.h"
#include "include/projection_enumerator.h"

#include "src/planner/logical_plan/include/logical_plan_util.h"
#include "src/planner/logical_plan/logical_operator/include/logical_accumulate.h"
#include "src/planner/logical_plan/logical_operator/include/logical_extend.h"
#include "src/planner/logical_plan/logical_operator/include/logical_ftable_scan.h"
#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"
#include "src/planner/logical_plan/logical_operator/include/sink_util.h"

namespace graphflow {
namespace planner {

static expression_vector getNewMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const expression_vector& expressions);
static expression_vector getNewMatchedExpressions(const SubqueryGraph& prevLeftSubgraph,
    const SubqueryGraph& prevRightSubgraph, const SubqueryGraph& newSubgraph,
    const expression_vector& expressions);

vector<unique_ptr<LogicalPlan>> JoinOrderEnumerator::enumerateJoinOrder(
    const QueryGraph& queryGraph, const shared_ptr<Expression>& queryGraphPredicate,
    vector<unique_ptr<LogicalPlan>> prevPlans) {
    context->init(queryGraph, queryGraphPredicate, std::move(prevPlans));
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

unique_ptr<JoinOrderEnumeratorContext> JoinOrderEnumerator::enterSubquery(
    LogicalPlan* outerPlan, expression_vector expressionsToScan) {
    auto prevContext = move(context);
    context = make_unique<JoinOrderEnumeratorContext>();
    context->outerPlan = outerPlan;
    context->expressionsToScanFromOuter = std::move(expressionsToScan);
    return prevContext;
}

void JoinOrderEnumerator::exitSubquery(unique_ptr<JoinOrderEnumeratorContext> prevContext) {
    context = move(prevContext);
}

void JoinOrderEnumerator::planOuterExpressionsScan(expression_vector& expressions) {
    auto newSubgraph = context->getEmptySubqueryGraph();
    for (auto& expression : expressions) {
        if (expression->getDataType().typeID == NODE_ID) {
            auto node = static_pointer_cast<NodeExpression>(expression->getChild(0));
            context->addMatchedNode(node.get());
            auto nodePos = context->getQueryGraph()->getQueryNodePos(*node);
            newSubgraph.addQueryNode(nodePos);
        }
    }
    auto plan = make_unique<LogicalPlan>();
    appendFTableScan(context->outerPlan, expressions, *plan);
    auto predicates = getNewMatchedExpressions(
        context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
    for (auto& predicate : predicates) {
        enumerator->appendFilter(predicate, *plan);
    }
    enumerator->projectionEnumerator.appendDistinct(expressions, *plan);
    context->addPlan(newSubgraph, std::move(plan));
}

void JoinOrderEnumerator::planNodeScan() {
    auto queryGraph = context->getQueryGraph();
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes(); ++nodePos) {
        auto node = queryGraph->getQueryNode(nodePos);
        if (context->isNodeMatched(node.get())) {
            continue;
        }
        context->addMatchedNode(node.get());
        auto newSubgraph = context->getEmptySubqueryGraph();
        newSubgraph.addQueryNode(nodePos);
        auto plan = make_unique<LogicalPlan>();

        appendScanNodeID(node, *plan);
        auto predicates = getNewMatchedExpressions(
            context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
        planFiltersForNode(predicates, *node, *plan);
        planPropertyScansForNode(*node, *plan);
        context->addPlan(newSubgraph, move(plan));
    }
}

void JoinOrderEnumerator::planFiltersForNode(
    expression_vector& predicates, NodeExpression& node, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        auto propertiesToScan = getPropertiesForVariable(*predicate, node);
        enumerator->appendScanNodePropIfNecessarySwitch(propertiesToScan, node, plan);
        enumerator->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::planPropertyScansForNode(NodeExpression& node, LogicalPlan& plan) {
    auto properties = enumerator->getPropertiesForNode(node);
    enumerator->appendScanNodePropIfNecessarySwitch(properties, node, plan);
}

void JoinOrderEnumerator::planRelScan() {
    auto queryGraph = context->getQueryGraph();
    for (auto relPos = 0u; relPos < queryGraph->getNumQueryRels(); ++relPos) {
        auto rel = queryGraph->getQueryRel(relPos);
        if (context->isRelMatched(rel.get())) {
            continue;
        }
        context->addMatchedRel(rel.get());
        auto newSubgraph = context->getEmptySubqueryGraph();
        newSubgraph.addQueryRel(relPos);
        auto predicates = getNewMatchedExpressions(
            context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
        for (auto direction : REL_DIRECTIONS) {
            auto plan = make_unique<LogicalPlan>();
            auto boundNode = direction == FWD ? rel->getSrcNode() : rel->getDstNode();
            appendScanNodeID(boundNode, *plan);
            planRelExtendFiltersAndProperties(rel, direction, predicates, *plan);
            context->addPlan(newSubgraph, move(plan));
        }
    }
}

void JoinOrderEnumerator::planFiltersForRel(
    expression_vector& predicates, RelExpression& rel, RelDirection direction, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        auto relPropertiesToScan = getPropertiesForVariable(*predicate, rel);
        enumerator->appendScanRelPropsIfNecessary(relPropertiesToScan, rel, direction, plan);
        enumerator->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::planPropertyScansForRel(
    RelExpression& rel, RelDirection direction, LogicalPlan& plan) {
    auto relProperties = enumerator->getPropertiesForRel(rel);
    enumerator->appendScanRelPropsIfNecessary(relProperties, rel, direction, plan);
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
    vector<shared_ptr<RelExpression>> rels, shared_ptr<NodeExpression> intersectNode) {
    auto newSubgraph = subgraph;
    vector<shared_ptr<NodeExpression>> boundNodes;
    vector<unique_ptr<LogicalPlan>> relPlans;
    for (auto& rel : rels) {
        auto boundNode = rel->getSrcNodeName() == intersectNode->getUniqueName() ?
                             rel->getDstNode() :
                             rel->getSrcNode();
        boundNodes.push_back(boundNode);
        auto relPos = context->getQueryGraph()->getQueryRelPos(rel->getUniqueName());
        newSubgraph.addQueryRel(relPos);
        // fetch build plans for rel
        auto relSubgraph = context->getEmptySubqueryGraph();
        relSubgraph.addQueryRel(relPos);
        assert(context->subPlansTable->containSubgraphPlans(relSubgraph));
        auto& relPlanCandidates = context->subPlansTable->getSubgraphPlans(relSubgraph);
        assert(relPlanCandidates.size() == 2); // 2 directions
        relPlans.push_back(getWCOJBuildPlanForRel(relPlanCandidates, *boundNode));
    }
    for (auto& leftPlan : context->getPlans(subgraph)) {
        auto leftPlanCopy = leftPlan->shallowCopy();
        vector<unique_ptr<LogicalPlan>> rightPlansCopy;
        for (auto& relPlan : relPlans) {
            rightPlansCopy.push_back(relPlan->shallowCopy());
        }
        appendIntersect(intersectNode, boundNodes, *leftPlanCopy, rightPlansCopy);
        context->subPlansTable->addPlan(newSubgraph, std::move(leftPlanCopy));
    }
}

void JoinOrderEnumerator::planInnerJoin(uint32_t leftLevel, uint32_t rightLevel) {
    assert(leftLevel <= rightLevel);
    for (auto& rightSubgraph : context->subPlansTable->getSubqueryGraphs(rightLevel)) {
        for (auto& nbrSubgraph : rightSubgraph.getNbrSubgraphs(leftLevel)) {
            // Consider previous example in enumerateExtend(), when enumerating second MATCH, and
            // current level = 4 we get left subgraph as f, d, e (size = 2), and try to find a
            // connected right subgraph of size 2. A possible right graph could be b, c, d. However,
            // b, c, d is a subgraph enumerated in the first MATCH and has been cleared before
            // enumeration of second MATCH. So subPlansTable does not contain this subgraph.
            if (!context->containPlans(nbrSubgraph)) {
                continue;
            }
            auto joinNodePositions = rightSubgraph.getConnectedNodePos(nbrSubgraph);
            auto joinNodes = context->mergedQueryGraph->getQueryNodes(joinNodePositions);
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
    assert(false);
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
        getNewMatchedExpressions(subgraph, newSubgraph, context->getWhereExpressions());
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
    auto predicates = getNewMatchedExpressions(
        subgraph, otherSubgraph, newSubgraph, context->getWhereExpressions());
    for (auto& leftPlan : context->getPlans(subgraph)) {
        for (auto& rightPlan : context->getPlans(otherSubgraph)) {
            auto leftPlanProbeCopy = leftPlan->shallowCopy();
            auto rightPlanBuildCopy = rightPlan->shallowCopy();
            auto leftPlanBuildCopy = leftPlan->shallowCopy();
            auto rightPlanProbeCopy = rightPlan->shallowCopy();
            planHashJoin(joinNodes, JoinType::INNER, false /* isProbeAcc */, *leftPlanProbeCopy,
                *rightPlanBuildCopy);
            planFiltersForHashJoin(predicates, *leftPlanProbeCopy);
            context->addPlan(newSubgraph, move(leftPlanProbeCopy));
            // flip build and probe side to get another HashJoin plan
            if (flipPlan) {
                planHashJoin(joinNodes, JoinType::INNER, false /* isProbeAcc */,
                    *rightPlanProbeCopy, *leftPlanBuildCopy);
                planFiltersForHashJoin(predicates, *rightPlanProbeCopy);
                context->addPlan(newSubgraph, move(rightPlanProbeCopy));
            }
        }
    }
}

void JoinOrderEnumerator::planFiltersForHashJoin(expression_vector& predicates, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        enumerator->appendFilter(predicate, plan);
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
    plan.appendOperator(std::move(fTableScan));
}

void JoinOrderEnumerator::appendScanNodeID(shared_ptr<NodeExpression>& node, LogicalPlan& plan) {
    assert(plan.isEmpty());
    auto schema = plan.getSchema();
    auto scan = make_shared<LogicalScanNodeID>(node);
    scan->computeSchema(*schema);
    plan.appendOperator(move(scan));
    // update cardinality estimation info
    auto numNodes = nodesStatisticsAndDeletedIDs.getNodeStatisticsAndDeletedIDs(node->getTableID())
                        ->getMaxNodeOffset() +
                    1;
    schema->getGroup(node->getIDProperty())->setMultiplier(numNodes);
}

void JoinOrderEnumerator::appendExtend(
    shared_ptr<RelExpression>& rel, RelDirection direction, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto boundNode = FWD == direction ? rel->getSrcNode() : rel->getDstNode();
    auto nbrNode = FWD == direction ? rel->getDstNode() : rel->getSrcNode();
    auto isColumn =
        catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(rel->getTableID(), direction);
    if (rel->isVariableLength() || !isColumn) {
        Enumerator::appendFlattenIfNecessary(boundNode->getNodeIDPropertyExpression(), plan);
    }
    auto extend = make_shared<LogicalExtend>(boundNode, nbrNode, rel->getTableID(), direction,
        isColumn, rel->getLowerBound(), rel->getUpperBound(), plan.getLastOperator());
    extend->computeSchema(*schema);
    plan.appendOperator(move(extend));
    // update cardinality estimation info
    if (!isColumn) {
        auto extensionRate =
            getExtensionRate(boundNode->getTableID(), rel->getTableID(), direction);
        schema->getGroup(nbrNode->getIDProperty())->setMultiplier(extensionRate);
    }
    plan.increaseCost(plan.getCardinality());
}

void JoinOrderEnumerator::planHashJoin(const vector<shared_ptr<NodeExpression>>& joinNodes,
    JoinType joinType, bool isProbeAcc, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    // TODO(Guodong): Fix asp for multiple join nodes.
    if (ASPOptimizer::canApplyASP(joinNodes, isProbeAcc, probePlan, buildPlan)) {
        ASPOptimizer::applyASP(joinNodes[0], probePlan, buildPlan);
        isProbeAcc = true;
    }
    appendHashJoin(joinNodes, joinType, isProbeAcc, probePlan, buildPlan);
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
    if (firstop->getLogicalOperatorType() != LOGICAL_SCAN_NODE_ID) {
        return false;
    }
    auto scanNodeID = (LogicalScanNodeID*)firstop;
    if (scanNodeID->getNode()->getIDProperty() != joinNodeID) {
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
        !isJoinKeyUniqueOnBuildSide(joinNodes[0]->getIDProperty(), buildPlan)) {
        for (auto& joinNode : joinNodes) {
            auto probeSideKeyGroupPos = probeSideSchema->getGroupPos(joinNode->getIDProperty());
            Enumerator::appendFlattenIfNecessary(probeSideKeyGroupPos, probePlan);
        }
        probePlan.multiplyCardinality(
            buildPlan.getCardinality() * EnumeratorKnobs::PREDICATE_SELECTIVITY);
        probePlan.multiplyCost(EnumeratorKnobs::FLAT_PROBE_PENALTY);
    }
    // Flat all but one build side key groups.
    unordered_set<uint32_t> joinNodesGroupPos;
    for (auto& joinNode : joinNodes) {
        joinNodesGroupPos.insert(buildSideSchema.getGroupPos(joinNode->getIDProperty()));
    }
    Enumerator::appendFlattensButOne(joinNodesGroupPos, buildPlan);

    auto numGroupsBeforeMerging = probeSideSchema->getNumGroups();
    vector<string> keys;
    for (auto& joinNode : joinNodes) {
        keys.push_back(joinNode->getIDProperty());
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
    probePlan.appendOperator(move(hashJoin));
}

void JoinOrderEnumerator::appendMarkJoin(const vector<shared_ptr<NodeExpression>>& joinNodes,
    shared_ptr<Expression>& mark, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    // TODO(Guodong): Remove restrictions of the num of join nodes.
    assert(joinNodes.size() == 1);
    auto joinNodeID = joinNodes[0]->getIDProperty();
    auto buildSchema = buildPlan.getSchema();
    auto probeSchema = probePlan.getSchema();
    probePlan.increaseCost(probePlan.getCardinality() + buildPlan.getCardinality());
    auto markGroupPos = probeSchema->getGroupPos(joinNodeID);
    probeSchema->insertToGroupAndScope(mark, markGroupPos);
    auto hashJoin = make_shared<LogicalHashJoin>(joinNodes, mark, buildSchema->copy(),
        probePlan.getLastOperator(), buildPlan.getLastOperator());
    probePlan.appendOperator(std::move(hashJoin));
}

void JoinOrderEnumerator::appendIntersect(const shared_ptr<NodeExpression>& intersectNode,
    vector<shared_ptr<NodeExpression>>& boundNodes, LogicalPlan& probePlan,
    vector<unique_ptr<LogicalPlan>>& buildPlans) {
    auto intersectNodeID = intersectNode->getIDProperty();
    auto probeSchema = probePlan.getSchema();
    auto logicalIntersect =
        make_shared<LogicalIntersect>(intersectNode, probePlan.getLastOperator());
    assert(boundNodes.size() == buildPlans.size());
    // Write intersect node and rels into a new group regardless of whether rel is n-n.
    auto outGroupPos = probeSchema->createGroup();
    // Write intersect node into output group.
    probeSchema->insertToGroupAndScope(intersectNode->getNodeIDPropertyExpression(), outGroupPos);
    for (auto i = 0u; i < buildPlans.size(); ++i) {
        auto boundNode = boundNodes[i];
        Enumerator::appendFlattenIfNecessary(
            probeSchema->getGroupPos(boundNode->getIDProperty()), probePlan);
        auto buildPlan = buildPlans[i].get();
        auto buildSchema = buildPlan->getSchema();
        auto expressions = buildSchema->getExpressionsInScope();
        // Write rel properties into output group.
        for (auto& expression : expressions) {
            if (expression->getUniqueName() == intersectNodeID ||
                expression->getUniqueName() == boundNode->getIDProperty()) {
                continue;
            }
            probeSchema->insertToGroupAndScope(expression, outGroupPos);
        }
        auto buildInfo =
            make_unique<LogicalIntersectBuildInfo>(boundNode, buildSchema->copy(), expressions);
        logicalIntersect->addChild(buildPlan->getLastOperator(), std::move(buildInfo));
    }
    probePlan.appendOperator(std::move(logicalIntersect));
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
    auto numRels = ((RelStatistics*)((*relsStatistics.getReadOnlyVersion())[relTableID].get()))
                       ->getNumRelsForDirectionBoundTable(relDirection, boundTableID);
    return ceil(
        (double)numRels / nodesStatisticsAndDeletedIDs.getNodeStatisticsAndDeletedIDs(boundTableID)
                              ->getMaxNodeOffset() +
        1);
}

expression_vector getNewMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const expression_vector& expressions) {
    expression_vector newMatchedExpressions;
    for (auto& expression : expressions) {
        auto includedVariables = expression->getDependentVariableNames();
        if (newSubgraph.containAllVariables(includedVariables) &&
            !prevSubgraph.containAllVariables(includedVariables)) {
            newMatchedExpressions.push_back(expression);
        }
    }
    return newMatchedExpressions;
}

expression_vector getNewMatchedExpressions(const SubqueryGraph& prevLeftSubgraph,
    const SubqueryGraph& prevRightSubgraph, const SubqueryGraph& newSubgraph,
    const expression_vector& expressions) {
    expression_vector newMatchedExpressions;
    for (auto& expression : expressions) {
        auto includedVariables = expression->getDependentVariableNames();
        if (newSubgraph.containAllVariables(includedVariables) &&
            !prevLeftSubgraph.containAllVariables(includedVariables) &&
            !prevRightSubgraph.containAllVariables(includedVariables)) {
            newMatchedExpressions.push_back(expression);
        }
    }
    return newMatchedExpressions;
}

} // namespace planner
} // namespace graphflow
