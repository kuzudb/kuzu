#include "src/planner/include/join_order_enumerator.h"

#include "src/planner/include/enumerator.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/intersect/logical_intersect.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/select_scan/logical_result_scan.h"

namespace graphflow {
namespace planner {

static vector<shared_ptr<Expression>> getNewMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const vector<shared_ptr<Expression>>& expressions);
static vector<shared_ptr<Expression>> getNewMatchedExpressions(
    const SubqueryGraph& prevLeftSubgraph, const SubqueryGraph& prevRightSubgraph,
    const SubqueryGraph& newSubgraph, const vector<shared_ptr<Expression>>& expressions);

// Rewrite a query rel that closes a cycle as a regular query rel for extend. This requires giving a
// different identifier to the node that will close the cycle. This identifier is created as rel
// name + node name.
static shared_ptr<RelExpression> rewriteQueryRel(const RelExpression& queryRel, bool isRewriteDst);
static shared_ptr<Expression> createNodeIDEqualComparison(
    const shared_ptr<NodeExpression>& left, const shared_ptr<NodeExpression>& right);

vector<unique_ptr<LogicalPlan>> JoinOrderEnumerator::enumerateJoinOrder(
    const QueryGraph& queryGraph, const shared_ptr<Expression>& queryGraphPredicate,
    vector<unique_ptr<LogicalPlan>> prevPlans) {
    context->init(queryGraph, queryGraphPredicate, move(prevPlans));
    context->hasExpressionsToScanFromOuter() ? enumerateResultScan() : enumerateSingleNode();
    while (context->hasNextLevel()) {
        context->incrementCurrentLevel();
        enumerateSingleRel();
        // TODO: remove currentLevel constraint for Hash Join enumeration
        if (context->getCurrentLevel() >= 4) {
            enumerateHashJoin();
        }
    }
    return move(context->getPlans(context->getFullyMatchedSubqueryGraph()));
}

unique_ptr<JoinOrderEnumeratorContext> JoinOrderEnumerator::enterSubquery(
    vector<shared_ptr<Expression>> expressionsToScan) {
    auto prevContext = move(context);
    context = make_unique<JoinOrderEnumeratorContext>();
    context->setExpressionsToScanFromOuter(move(expressionsToScan));
    return prevContext;
}

void JoinOrderEnumerator::exitSubquery(unique_ptr<JoinOrderEnumeratorContext> prevContext) {
    context = move(prevContext);
}

void JoinOrderEnumerator::enumerateResultScan() {
    auto emptySubgraph = context->getEmptySubqueryGraph();
    auto plan = context->containPlans(emptySubgraph) ? context->getPlans(emptySubgraph)[0]->copy() :
                                                       make_unique<LogicalPlan>();
    auto newSubgraph = emptySubgraph;
    unordered_set<string> expressionNamesToSelect;
    for (auto& expression : context->getExpressionsToScanFromOuter()) {
        expressionNamesToSelect.insert(expression->getUniqueName());
        if (expression->dataType == NODE_ID) {
            assert(expression->expressionType == PROPERTY);
            newSubgraph.addQueryNode(context->getQueryGraph()->getQueryNodePos(
                expression->getChild(0)->getUniqueName()));
        }
    }
    appendResultScan(expressionNamesToSelect, *plan);
    for (auto& expression :
        getNewMatchedExpressions(emptySubgraph, newSubgraph, context->getWhereExpressions())) {
        enumerator->appendFilter(expression, *plan);
    }
    context->addPlan(newSubgraph, move(plan));
}

void JoinOrderEnumerator::enumerateSingleNode() {
    auto emptySubgraph = context->getEmptySubqueryGraph();
    auto prevPlan = context->containPlans(emptySubgraph) ?
                        context->getPlans(emptySubgraph)[0]->copy() :
                        make_unique<LogicalPlan>();
    if (context->getMatchedQueryNodes().count() == 1) {
        // If only single node has been previously enumerated, then join order is decided
        return;
    }
    auto queryGraph = context->getQueryGraph();
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes(); ++nodePos) {
        auto newSubgraph = context->getEmptySubqueryGraph();
        newSubgraph.addQueryNode(nodePos);
        auto plan = prevPlan->copy();
        auto& node = *queryGraph->queryNodes[nodePos];
        appendScanNodeID(node, *plan);
        for (auto& expression :
            getNewMatchedExpressions(emptySubgraph, newSubgraph, context->getWhereExpressions())) {
            enumerator->appendFilter(expression, *plan);
        }
        context->addPlan(newSubgraph, move(plan));
    }
}

void JoinOrderEnumerator::enumerateSingleRel() {
    auto queryGraph = context->getQueryGraph();
    auto& subgraphPlansMap = context->getSubqueryGraphPlansMap(context->getCurrentLevel() - 1);
    for (auto& [prevSubgraph, prevPlans] : subgraphPlansMap) {
        auto connectedQueryRelsWithDirection =
            queryGraph->getConnectedQueryRelsWithDirection(prevSubgraph);
        for (auto& [relPos, isSrcMatched, isDstMatched] : connectedQueryRelsWithDirection) {
            // Consider query MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) WITH *
            // MATCH (d)->[r4]->(e)-[r5]->(f) RETURN *
            // First MATCH is enumerated normally. When enumerating second MATCH,
            // we first merge graph as (a)-[r1]->(b)-[r2]->(c)-[r3]->(d)->[r4]->(e)-[r5]->(f)
            // and enumerate from level 0 again. If we hit a query rel that has been
            // previously matched i.e. r1 & r2 & r3, we skip the plan. This guarantees DP only
            // enumerate query rels in the second MATCH.
            // Note this is different from fully merged, since we don't generate plans like
            // build side QVO : a, b, c,  probe side QVO: f, e, d, c, HashJoin(c).
            if (context->getMatchedQueryRels()[relPos]) {
                continue;
            }
            auto newSubgraph = prevSubgraph;
            newSubgraph.addQueryRel(relPos);
            auto expressionsToFilter =
                getNewMatchedExpressions(prevSubgraph, newSubgraph, context->getWhereExpressions());
            auto& queryRel = *queryGraph->queryRels[relPos];
            if (isSrcMatched && isDstMatched) {
                // TODO: refactor cyclic logic as a separate function
                for (auto& prevPlan : prevPlans) {
                    for (auto direction : DIRECTIONS) {
                        auto isCloseOnDst = direction == FWD;
                        // Break cycle by creating a temporary rel with a different name (concat rel
                        // and node name) on closing node.
                        auto tmpRel = rewriteQueryRel(queryRel, isCloseOnDst);
                        auto nodeToIntersect = isCloseOnDst ? queryRel.dstNode : queryRel.srcNode;
                        auto tmpNode = isCloseOnDst ? tmpRel->dstNode : tmpRel->srcNode;

                        auto planWithFilter = prevPlan->copy();
                        appendExtendFiltersAndScanPropertiesIfNecessary(
                            *tmpRel, direction, expressionsToFilter, *planWithFilter);
                        auto nodeIDFilter = createNodeIDEqualComparison(nodeToIntersect, tmpNode);
                        enumerator->appendFilter(nodeIDFilter, *planWithFilter);
                        context->addPlan(newSubgraph, move(planWithFilter));

                        auto planWithIntersect = prevPlan->copy();
                        appendExtendFiltersAndScanPropertiesIfNecessary(
                            *tmpRel, direction, expressionsToFilter, *planWithIntersect);
                        if (appendIntersect(nodeToIntersect->getIDProperty(),
                                tmpNode->getIDProperty(), *planWithIntersect)) {
                            context->addPlan(newSubgraph, move(planWithIntersect));
                        }
                    }
                }
            } else {
                for (auto& prevPlan : prevPlans) {
                    auto plan = prevPlan->copy();
                    appendExtendFiltersAndScanPropertiesIfNecessary(
                        queryRel, isSrcMatched ? FWD : BWD, expressionsToFilter, *plan);
                    context->addPlan(newSubgraph, move(plan));
                }
            }
        }
    }
}

void JoinOrderEnumerator::enumerateHashJoin() {
    auto currentLevel = context->getCurrentLevel();
    auto queryGraph = context->getQueryGraph();
    for (auto leftSize = currentLevel - 2; leftSize >= ceil(currentLevel / 2.0); --leftSize) {
        auto& subgraphPlansMap = context->getSubqueryGraphPlansMap(leftSize);
        for (auto& [leftSubgraph, leftPlans] : subgraphPlansMap) {
            auto rightSubgraphAndJoinNodePairs =
                queryGraph->getSingleNodeJoiningSubgraph(leftSubgraph, currentLevel - leftSize);
            for (auto& [rightSubgraph, joinNodePos] : rightSubgraphAndJoinNodePairs) {
                // Consider previous example in enumerateExtend()
                // When enumerating second MATCH, and current level = 4
                // we get left subgraph as f, d, e (size = 2), and try to find a connected
                // right subgraph of size 2. A possible right graph could be b, c, d.
                // However, b, c, d is a subgraph enumerated in the first MATCH and has been
                // cleared before enumeration of second MATCH. So subPlansTable does not
                // contain this subgraph.
                if (!context->containPlans(rightSubgraph)) {
                    continue;
                }
                auto& rightPlans = context->getPlans(rightSubgraph);
                auto newSubgraph = leftSubgraph;
                newSubgraph.addSubqueryGraph(rightSubgraph);
                auto& joinNode = *queryGraph->queryNodes[joinNodePos];
                auto expressionsToFilter = getNewMatchedExpressions(
                    leftSubgraph, rightSubgraph, newSubgraph, context->getWhereExpressions());
                for (auto& leftPlan : leftPlans) {
                    for (auto& rightPlan : rightPlans) {
                        auto probePlan = leftPlan->copy();
                        appendLogicalHashJoin(joinNode, *probePlan, *rightPlan);
                        for (auto& expression : expressionsToFilter) {
                            enumerator->appendFilter(expression, *probePlan);
                        }
                        context->addPlan(newSubgraph, move(probePlan));
                        // flip build and probe side to get another HashJoin plan
                        if (leftSize != currentLevel - leftSize) {
                            probePlan = rightPlan->copy();
                            appendLogicalHashJoin(joinNode, *probePlan, *leftPlan);
                            for (auto& expression : expressionsToFilter) {
                                enumerator->appendFilter(expression, *probePlan);
                            }
                            context->addPlan(newSubgraph, move(probePlan));
                        }
                    }
                }
            }
        }
    }
}

void JoinOrderEnumerator::appendResultScan(
    const unordered_set<string>& expressionNamesToSelect, LogicalPlan& plan) {
    auto resultScan = make_shared<LogicalResultScan>(expressionNamesToSelect);
    auto groupPos = plan.schema->createGroup();
    for (auto& expressionToSelect : expressionNamesToSelect) {
        plan.schema->insertToGroupAndScope(expressionToSelect, groupPos);
    }
    plan.schema->groups[groupPos]->estimatedCardinality = 1;
    plan.schema->groups[groupPos]->isFlat = true;
    plan.appendOperator(move(resultScan));
}

void JoinOrderEnumerator::appendScanNodeID(const NodeExpression& queryNode, LogicalPlan& plan) {
    auto nodeID = queryNode.getIDProperty();
    assert(plan.isEmpty());
    auto scan = make_shared<LogicalScanNodeID>(nodeID, queryNode.label);
    auto groupPos = plan.schema->createGroup();
    plan.schema->insertToGroupAndScope(nodeID, groupPos);
    plan.schema->groups[groupPos]->estimatedCardinality = graph.getNumNodes(queryNode.label);
    plan.appendOperator(move(scan));
}

void JoinOrderEnumerator::appendExtendFiltersAndScanPropertiesIfNecessary(
    const RelExpression& queryRel, Direction direction,
    const vector<shared_ptr<Expression>>& expressionsToFilter, LogicalPlan& plan) {
    appendExtend(queryRel, direction, plan);
    for (auto& expression : expressionsToFilter) {
        enumerator->appendFilter(expression, plan);
    }
    auto nbrNode = FWD == direction ? queryRel.dstNode : queryRel.srcNode;
}

void JoinOrderEnumerator::appendExtend(
    const RelExpression& queryRel, Direction direction, LogicalPlan& plan) {
    auto boundNode = FWD == direction ? queryRel.srcNode : queryRel.dstNode;
    auto nbrNode = FWD == direction ? queryRel.dstNode : queryRel.srcNode;
    auto boundNodeID = boundNode->getIDProperty();
    auto nbrNodeID = nbrNode->getIDProperty();
    auto isColumnExtend =
        graph.getCatalog().isSingleMultiplicityInDirection(queryRel.label, direction);
    uint32_t groupPos;
    if (isColumnExtend) {
        groupPos = plan.schema->getGroupPos(boundNodeID);
    } else {
        auto boundNodeGroupPos = plan.schema->getGroupPos(boundNodeID);
        enumerator->appendFlattenIfNecessary(boundNodeGroupPos, plan);
        groupPos = plan.schema->createGroup();
        plan.schema->groups[groupPos]->estimatedCardinality =
            plan.schema->groups[boundNodeGroupPos]->estimatedCardinality *
            getExtensionRate(boundNode->label, queryRel.label, direction);
    }
    auto extend = make_shared<LogicalExtend>(boundNodeID, boundNode->label, nbrNodeID,
        nbrNode->label, queryRel.label, direction, isColumnExtend, queryRel.lowerBound,
        queryRel.upperBound, plan.lastOperator);
    plan.schema->addLogicalExtend(queryRel.getUniqueName(), extend.get());
    plan.schema->insertToGroupAndScope(nbrNodeID, groupPos);
    plan.appendOperator(move(extend));
}

void JoinOrderEnumerator::appendLogicalHashJoin(
    const NodeExpression& joinNode, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto joinNodeID = joinNode.getIDProperty();
    // Flat probe side key group if necessary
    auto probeSideKeyGroupPos = probePlan.schema->getGroupPos(joinNodeID);
    auto probeSideKeyGroup = probePlan.schema->groups[probeSideKeyGroupPos].get();
    enumerator->appendFlattenIfNecessary(probeSideKeyGroupPos, probePlan);
    // Merge key group from build side into probe side.
    auto& buildSideSchema = *buildPlan.schema;
    auto buildSideKeyGroupPos = buildSideSchema.getGroupPos(joinNodeID);
    probeSideKeyGroup->estimatedCardinality = max(probeSideKeyGroup->estimatedCardinality,
        buildSideSchema.groups[buildSideKeyGroupPos]->estimatedCardinality);
    probePlan.schema->insertToGroupAndScope(
        buildSideSchema.getExpressionNamesInScope(buildSideKeyGroupPos), probeSideKeyGroupPos);
    // Merge payload groups
    unordered_set<uint32_t> payloadGroupsPos;
    for (auto& groupPos : buildSideSchema.getGroupsPosInScope()) {
        if (groupPos == buildSideKeyGroupPos) {
            continue;
        }
        payloadGroupsPos.insert(groupPos);
    }
    Enumerator::computeSchemaForHashJoinAndOrderBy(
        payloadGroupsPos, buildSideSchema, *probePlan.schema);

    auto hashJoin = make_shared<LogicalHashJoin>(joinNodeID, buildSideSchema.copy(),
        buildSideSchema.getExpressionNamesInScope(), probePlan.lastOperator,
        buildPlan.lastOperator);
    probePlan.appendOperator(move(hashJoin));
}

bool JoinOrderEnumerator::appendIntersect(
    const string& leftNodeID, const string& rightNodeID, LogicalPlan& plan) {
    auto leftGroupPos = plan.schema->getGroupPos(leftNodeID);
    auto rightGroupPos = plan.schema->getGroupPos(rightNodeID);
    auto& leftGroup = *plan.schema->groups[leftGroupPos];
    auto& rightGroup = *plan.schema->groups[rightGroupPos];
    if (leftGroup.isFlat || rightGroup.isFlat) {
        // We should use filter close cycle if any group is flat.
        return false;
    }
    auto intersect = make_shared<LogicalIntersect>(leftNodeID, rightNodeID, plan.lastOperator);
    plan.cost += max(leftGroup.estimatedCardinality, rightGroup.estimatedCardinality);
    leftGroup.estimatedCardinality *= PREDICATE_SELECTIVITY;
    rightGroup.estimatedCardinality *= PREDICATE_SELECTIVITY;
    plan.appendOperator(move(intersect));
    return true;
}

uint64_t JoinOrderEnumerator::getExtensionRate(
    label_t boundNodeLabel, label_t relLabel, Direction direction) {
    auto numRels = graph.getNumRelsForDirBoundLabelRelLabel(direction, boundNodeLabel, relLabel);
    return ceil((double)numRels / graph.getNumNodes(boundNodeLabel));
}

vector<shared_ptr<Expression>> getNewMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const vector<shared_ptr<Expression>>& expressions) {
    vector<shared_ptr<Expression>> newMatchedExpressions;
    for (auto& expression : expressions) {
        auto includedVariables = expression->getDependentVariableNames();
        if (newSubgraph.containAllVariables(includedVariables) &&
            !prevSubgraph.containAllVariables(includedVariables)) {
            newMatchedExpressions.push_back(expression);
        }
    }
    return newMatchedExpressions;
}

vector<shared_ptr<Expression>> getNewMatchedExpressions(const SubqueryGraph& prevLeftSubgraph,
    const SubqueryGraph& prevRightSubgraph, const SubqueryGraph& newSubgraph,
    const vector<shared_ptr<Expression>>& expressions) {
    vector<shared_ptr<Expression>> newMatchedExpressions;
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

shared_ptr<RelExpression> rewriteQueryRel(const RelExpression& queryRel, bool isRewriteDst) {
    auto& nodeToRewrite = isRewriteDst ? *queryRel.dstNode : *queryRel.srcNode;
    auto tmpNode = make_shared<NodeExpression>(
        nodeToRewrite.getUniqueName() + "_" + queryRel.getUniqueName(), nodeToRewrite.label);
    return make_shared<RelExpression>(queryRel.getUniqueName(), queryRel.label,
        isRewriteDst ? queryRel.srcNode : tmpNode, isRewriteDst ? tmpNode : queryRel.dstNode,
        queryRel.lowerBound, queryRel.upperBound);
}

shared_ptr<Expression> createNodeIDEqualComparison(
    const shared_ptr<NodeExpression>& left, const shared_ptr<NodeExpression>& right) {
    auto srcNodeID = make_shared<PropertyExpression>(
        NODE_ID, INTERNAL_ID_SUFFIX, UINT32_MAX /* property key for internal id */, left);
    auto dstNodeID = make_shared<PropertyExpression>(
        NODE_ID, INTERNAL_ID_SUFFIX, UINT32_MAX /* property key for internal id  */, right);
    return make_shared<Expression>(EQUALS_NODE_ID, BOOL, move(srcNodeID), move(dstNodeID));
}

} // namespace planner
} // namespace graphflow
