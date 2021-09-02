#include "src/planner/include/join_order_enumerator.h"

#include "src/planner/include/enumerator.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/intersect/logical_intersect.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"

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
    const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans) {
    initStatus(queryPart, move(prevPlans));
    enumerateSingleNode();
    while (currentLevel < mergedQueryGraph->getNumQueryRels()) {
        currentLevel++;
        enumerateSingleRel();
        // TODO: remove currentLevel constraint for Hash Join enumeration
        if (currentLevel >= 4) {
            enumerateHashJoin();
        }
    }
    return move(subPlansTable->getSubgraphPlans(getFullyMatchedSubqueryGraph()));
}

void JoinOrderEnumerator::initStatus(
    const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans) {
    populatePropertiesMap(queryPart);
    appendMissingPropertyScans(prevPlans);
    // split where expression
    whereExpressionsSplitOnAND = queryPart.hasWhereExpression() ?
                                     queryPart.getWhereExpression()->splitOnAND() :
                                     vector<shared_ptr<Expression>>();
    // merge new query graph
    auto fullyMatchedSubqueryGraph = getFullyMatchedSubqueryGraph();
    matchedQueryRels = fullyMatchedSubqueryGraph.queryRelsSelector;
    matchedQueryNodes = fullyMatchedSubqueryGraph.queryNodesSelector;
    mergedQueryGraph->merge(*queryPart.getQueryGraph());
    // clear and resize subPlansTable
    subPlansTable->clear();
    subPlansTable->resize(mergedQueryGraph->getNumQueryRels());
    // add plans from previous query part into subPlanTable
    for (auto& prevPlan : prevPlans) {
        subPlansTable->addPlan(fullyMatchedSubqueryGraph, move(prevPlan));
    }
    // Restart from level 0 for new query part so that we get hashJoin based plans
    // that uses subplans coming from previous query part.See example in enumerateExtend().
    currentLevel = 0;
}

void JoinOrderEnumerator::populatePropertiesMap(const NormalizedQueryPart& queryPart) {
    variableToPropertiesMap.clear();
    for (auto& propertyExpression : queryPart.getDependentProperties()) {
        auto variableName = propertyExpression->children[0]->getInternalName();
        if (!variableToPropertiesMap.contains(variableName)) {
            variableToPropertiesMap.insert({variableName, vector<shared_ptr<Expression>>()});
        }
        variableToPropertiesMap.at(variableName).push_back(propertyExpression);
    }
}

void JoinOrderEnumerator::appendMissingPropertyScans(const vector<unique_ptr<LogicalPlan>>& plans) {
    // E.g. MATCH (a) WITH a MATCH (a)->(b) RETRUN a.age
    // a.age is not available in the scope of first query part. So append the missing properties
    // before starting join order enumeration of the second part.
    for (auto& plan : plans) {
        for (auto& [variableName, properties] : variableToPropertiesMap) {
            if (mergedQueryGraph->containsQueryNode(variableName)) {
                appendScanPropertiesIfNecessary(variableName, true, *plan);
            } else if (mergedQueryGraph->containsQueryRel(variableName)) {
                appendScanPropertiesIfNecessary(variableName, false, *plan);
            }
        }
    }
}

void JoinOrderEnumerator::enumerateSingleNode() {
    auto emptySubgraph = SubqueryGraph(*mergedQueryGraph);
    auto prevPlan = subPlansTable->containSubgraphPlans(emptySubgraph) ?
                        subPlansTable->getSubgraphPlans(emptySubgraph)[0]->copy() :
                        make_unique<LogicalPlan>();
    if (matchedQueryNodes.count() == 1) {
        // If only single node has been previously enumerated, then join order is decided
        return;
    }
    for (auto nodePos = 0u; nodePos < mergedQueryGraph->getNumQueryNodes(); ++nodePos) {
        auto newSubgraph = SubqueryGraph(*mergedQueryGraph);
        newSubgraph.addQueryNode(nodePos);
        auto plan = prevPlan->copy();
        auto& node = *mergedQueryGraph->queryNodes[nodePos];
        appendScanNodeID(node, *plan);
        for (auto& expression :
            getNewMatchedExpressions(emptySubgraph, newSubgraph, whereExpressionsSplitOnAND)) {
            Enumerator::appendFilter(expression, *plan);
        }
        appendScanPropertiesIfNecessary(node.getInternalName(), true /* isNode */, *plan);
        subPlansTable->addPlan(newSubgraph, move(plan));
    }
}

void JoinOrderEnumerator::enumerateSingleRel() {
    auto& subgraphPlansMap = subPlansTable->getSubqueryGraphPlansMap(currentLevel - 1);
    for (auto& [prevSubgraph, prevPlans] : subgraphPlansMap) {
        auto connectedQueryRelsWithDirection =
            mergedQueryGraph->getConnectedQueryRelsWithDirection(prevSubgraph);
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
            if (matchedQueryRels[relPos]) {
                continue;
            }
            auto newSubgraph = prevSubgraph;
            newSubgraph.addQueryRel(relPos);
            auto expressionsToFilter =
                getNewMatchedExpressions(prevSubgraph, newSubgraph, whereExpressionsSplitOnAND);
            auto& queryRel = *mergedQueryGraph->queryRels[relPos];
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
                        appendExtendAndFiltersIfNecessary(
                            *tmpRel, direction, expressionsToFilter, *planWithFilter);
                        auto nodeIDFilter = createNodeIDEqualComparison(nodeToIntersect, tmpNode);
                        Enumerator::appendFilter(move(nodeIDFilter), *planWithFilter);
                        subPlansTable->addPlan(newSubgraph, move(planWithFilter));

                        auto planWithIntersect = prevPlan->copy();
                        appendExtendAndFiltersIfNecessary(
                            *tmpRel, direction, expressionsToFilter, *planWithIntersect);
                        if (appendIntersect(nodeToIntersect->getIDProperty(),
                                tmpNode->getIDProperty(), *planWithIntersect)) {
                            subPlansTable->addPlan(newSubgraph, move(planWithIntersect));
                        }
                    }
                }
            } else {
                for (auto& prevPlan : prevPlans) {
                    auto plan = prevPlan->copy();
                    appendExtendAndFiltersIfNecessary(
                        queryRel, isSrcMatched ? FWD : BWD, expressionsToFilter, *plan);
                    subPlansTable->addPlan(newSubgraph, move(plan));
                }
            }
        }
    }
}

void JoinOrderEnumerator::enumerateHashJoin() {
    for (auto leftSize = currentLevel - 2; leftSize >= ceil(currentLevel / 2.0); --leftSize) {
        auto& subgraphPlansMap = subPlansTable->getSubqueryGraphPlansMap(leftSize);
        for (auto& [leftSubgraph, leftPlans] : subgraphPlansMap) {
            auto rightSubgraphAndJoinNodePairs = mergedQueryGraph->getSingleNodeJoiningSubgraph(
                leftSubgraph, currentLevel - leftSize);
            for (auto& [rightSubgraph, joinNodePos] : rightSubgraphAndJoinNodePairs) {
                // Consider previous example in enumerateExtend()
                // When enumerating second MATCH, and current level = 4
                // we get left subgraph as f, d, e (size = 2), and try to find a connected
                // right subgraph of size 2. A possible right graph could be b, c, d.
                // However, b, c, d is a subgraph enumerated in the first MATCH and has been
                // cleared before enumeration of second MATCH. So subPlansTable does not
                // contain this subgraph.
                if (!subPlansTable->containSubgraphPlans(rightSubgraph)) {
                    continue;
                }
                auto& rightPlans = subPlansTable->getSubgraphPlans(rightSubgraph);
                auto newSubgraph = leftSubgraph;
                newSubgraph.addSubqueryGraph(rightSubgraph);
                auto& joinNode = *mergedQueryGraph->queryNodes[joinNodePos];
                auto expressionsToFilter = getNewMatchedExpressions(
                    leftSubgraph, rightSubgraph, newSubgraph, whereExpressionsSplitOnAND);
                for (auto& leftPlan : leftPlans) {
                    for (auto& rightPlan : rightPlans) {
                        auto plan = leftPlan->copy();
                        appendLogicalHashJoin(joinNode, *rightPlan, *plan);
                        for (auto& expression : expressionsToFilter) {
                            Enumerator::appendFilter(expression, *plan);
                        }
                        subPlansTable->addPlan(newSubgraph, move(plan));
                        // flip build and probe side to get another HashJoin plan
                        if (leftSize != currentLevel - leftSize) {
                            auto planFlipped = rightPlan->copy();
                            appendLogicalHashJoin(joinNode, *leftPlan, *planFlipped);
                            for (auto& expression : expressionsToFilter) {
                                Enumerator::appendFilter(expression, *planFlipped);
                            }
                            subPlansTable->addPlan(newSubgraph, move(planFlipped));
                        }
                    }
                }
            }
        }
    }
}

void JoinOrderEnumerator::appendScanNodeID(const NodeExpression& queryNode, LogicalPlan& plan) {
    auto nodeID = queryNode.getIDProperty();
    auto scan = plan.lastOperator ?
                    make_shared<LogicalScanNodeID>(nodeID, queryNode.label, plan.lastOperator) :
                    make_shared<LogicalScanNodeID>(nodeID, queryNode.label);
    auto groupPos = plan.schema->createGroup();
    plan.schema->appendToGroup(nodeID, groupPos);
    plan.schema->groups[groupPos]->estimatedCardinality = graph.getNumNodes(queryNode.label);
    plan.appendOperator(move(scan));
}

void JoinOrderEnumerator::appendExtendAndFiltersIfNecessary(const RelExpression& queryRel,
    Direction direction, const vector<shared_ptr<Expression>>& expressionsToFilter,
    LogicalPlan& plan) {
    appendExtend(queryRel, direction, plan);
    for (auto& expression : expressionsToFilter) {
        Enumerator::appendFilter(expression, plan);
    }
    auto nbrNode = FWD == direction ? queryRel.dstNode : queryRel.srcNode;
    appendScanPropertiesIfNecessary(nbrNode->getInternalName(), true /* isNode */, plan);
    appendScanPropertiesIfNecessary(queryRel.getInternalName(), false /* isNode */, plan);
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
        Enumerator::appendFlattenIfNecessary(boundNodeGroupPos, plan);
        groupPos = plan.schema->createGroup();
        plan.schema->groups[groupPos]->estimatedCardinality =
            plan.schema->groups[boundNodeGroupPos]->estimatedCardinality *
            getExtensionRate(boundNode->label, queryRel.label, direction);
    }
    auto extend = make_shared<LogicalExtend>(boundNodeID, boundNode->label, nbrNodeID,
        nbrNode->label, queryRel.label, direction, isColumnExtend, queryRel.lowerBound,
        queryRel.upperBound, plan.lastOperator);
    plan.schema->addLogicalExtend(queryRel.getInternalName(), extend.get());
    plan.schema->appendToGroup(nbrNodeID, groupPos);
    plan.appendOperator(move(extend));
}

void JoinOrderEnumerator::appendLogicalHashJoin(
    const NodeExpression& joinNode, LogicalPlan& buildPlan, LogicalPlan& probePlan) {
    auto joinNodeID = joinNode.getIDProperty();
    // Flat probe side key group if necessary
    auto probeSideKeyGroupPos = probePlan.schema->getGroupPos(joinNodeID);
    auto probeSideKeyGroup = probePlan.schema->groups[probeSideKeyGroupPos].get();
    if (!probeSideKeyGroup->isFlat) {
        Enumerator::appendFlattenIfNecessary(probeSideKeyGroupPos, probePlan);
    }
    // Build side: 1) append non-key vectors in the key data chunk into probe side key data chunk.
    auto buildSideKeyGroupPos = buildPlan.schema->getGroupPos(joinNodeID);
    auto buildSideKeyGroup = buildPlan.schema->groups[buildSideKeyGroupPos].get();
    probeSideKeyGroup->estimatedCardinality =
        max(probeSideKeyGroup->estimatedCardinality, buildSideKeyGroup->estimatedCardinality);
    for (auto& variable : buildSideKeyGroup->variables) {
        if (variable == joinNodeID) {
            continue; // skip the key vector because it exists on probe side
        }
        probePlan.schema->appendToGroup(variable, probeSideKeyGroupPos);
    }

    auto allGroupsOnBuildSideIsFlat = true;
    // the appended probe side group that holds all flat groups on build side
    auto probeSideAppendedFlatGroupPos = UINT32_MAX;
    for (auto i = 0u; i < buildPlan.schema->groups.size(); ++i) {
        if (i == buildSideKeyGroupPos) {
            continue;
        }
        auto& buildSideGroup = buildPlan.schema->groups[i];
        if (buildSideGroup->isFlat) {
            // 2) append flat non-key data chunks into buildSideNonKeyFlatDataChunk.
            if (probeSideAppendedFlatGroupPos == UINT32_MAX) {
                probeSideAppendedFlatGroupPos = probePlan.schema->createGroup();
            }
            probePlan.schema->appendToGroup(*buildSideGroup, probeSideAppendedFlatGroupPos);
        } else {
            // 3) append unflat non-key data chunks as new data chunks into dataChunks.
            allGroupsOnBuildSideIsFlat = false;
            auto groupPos = probePlan.schema->createGroup();
            probePlan.schema->appendToGroup(*buildSideGroup, groupPos);
            probePlan.schema->groups[groupPos]->estimatedCardinality =
                buildSideGroup->estimatedCardinality;
        }
    }

    if (!allGroupsOnBuildSideIsFlat && probeSideAppendedFlatGroupPos != UINT32_MAX) {
        probePlan.schema->flattenGroup(probeSideAppendedFlatGroupPos);
    }
    auto hashJoin = make_shared<LogicalHashJoin>(
        joinNodeID, buildPlan.lastOperator, buildPlan.schema->copy(), probePlan.lastOperator);
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

void JoinOrderEnumerator::appendScanPropertiesIfNecessary(
    const string& variableName, bool isNode, LogicalPlan& plan) {
    if (!variableToPropertiesMap.contains(variableName)) {
        return;
    }
    for (auto& expression : variableToPropertiesMap.at(variableName)) {
        auto& propertyExpression = (PropertyExpression&)*expression;
        if (plan.schema->containVariable(propertyExpression.getInternalName())) {
            continue;
        }
        isNode ? Enumerator::appendScanNodeProperty(propertyExpression, plan) :
                 Enumerator::appendScanRelProperty(propertyExpression, plan);
    }
}

/**
 * Returns a SubqueryGraph, which is a class used as a key in the subPlanTable, for
 * the MergedQueryGraph when all of its nodes and rels are matched
 */
SubqueryGraph JoinOrderEnumerator::getFullyMatchedSubqueryGraph() {
    auto matchedSubgraph = SubqueryGraph(*mergedQueryGraph);
    for (auto i = 0u; i < mergedQueryGraph->getNumQueryNodes(); ++i) {
        matchedSubgraph.addQueryNode(i);
    }
    for (auto i = 0u; i < mergedQueryGraph->getNumQueryRels(); ++i) {
        matchedSubgraph.addQueryRel(i);
    }
    return matchedSubgraph;
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
        nodeToRewrite.getInternalName() + "_" + queryRel.getInternalName(), nodeToRewrite.label);
    return make_shared<RelExpression>(queryRel.getInternalName(), queryRel.label,
        isRewriteDst ? queryRel.srcNode : tmpNode, isRewriteDst ? tmpNode : queryRel.dstNode,
        queryRel.lowerBound, queryRel.upperBound);
}

shared_ptr<Expression> createNodeIDEqualComparison(
    const shared_ptr<NodeExpression>& left, const shared_ptr<NodeExpression>& right) {
    auto srcNodeID = make_shared<PropertyExpression>(
        NODE_ID, INTERNAL_ID_SUFFIX, UINT32_MAX /* property key for internal id */, left);
    auto dstNodeID = make_shared<PropertyExpression>(
        NODE_ID, INTERNAL_ID_SUFFIX, UINT32_MAX /* property key for internal id  */, right);
    auto result = make_shared<Expression>(EQUALS_NODE_ID, BOOL, move(srcNodeID), move(dstNodeID));
    result->rawExpression =
        result->children[0]->getInternalName() + " = " + result->children[1]->getInternalName();
    return result;
}

} // namespace planner
} // namespace graphflow
