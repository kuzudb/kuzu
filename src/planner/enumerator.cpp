#include "src/planner/include/enumerator.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"

namespace graphflow {
namespace planner {

static vector<shared_ptr<LogicalExpression>> getNewMatchedWhereExpressions(
    const SubqueryGraph& prevSubgraph, const SubqueryGraph& newSubgraph,
    vector<pair<shared_ptr<LogicalExpression>, unordered_set<string>>>&
        expressionsAndIncludedVariables);

static vector<shared_ptr<LogicalExpression>> getNewMatchedWhereExpressions(
    const SubqueryGraph& prevLeftSubgraph, const SubqueryGraph& prevRightSubgraph,
    const SubqueryGraph& newSubgraph,
    vector<pair<shared_ptr<LogicalExpression>, unordered_set<string>>>&
        expressionsAndIncludedVariables);

static pair<string, string> splitVariableAndPropertyName(const string& name);

Enumerator::Enumerator(const BoundSingleQuery& boundSingleQuery)
    : queryGraph{*boundSingleQuery.queryGraph} {
    subgraphPlanTable = make_unique<SubgraphPlanTable>(queryGraph.queryRels.size());
    if (!boundSingleQuery.whereExpressionsSplitOnAND.empty()) {
        for (auto& expression : boundSingleQuery.whereExpressionsSplitOnAND) {
            whereClauseAndIncludedVariables.emplace_back(
                make_pair(expression, expression->getIncludedVariables()));
        }
    }
    returnClause = move(boundSingleQuery.boundReturnStatement->expressionsToReturn);
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans() {
    auto numEnumeratedQueryRels = 0u;
    enumerateSingleQueryNode();
    while (numEnumeratedQueryRels < queryGraph.queryRels.size()) {
        enumerateNextNumQueryRel(numEnumeratedQueryRels);
    }
    auto finalPlans =
        move(subgraphPlanTable->subgraphPlans[queryGraph.queryRels.size()].begin()->second);
    // Do not append projection until back-end support
    if (auto APPEND_PROJECTION = false) {
        for (auto& plan : finalPlans) {
            appendProjection(*plan);
        }
    }
    return finalPlans;
}

void Enumerator::enumerateSingleQueryNode() {
    for (auto nodePos = 0u; nodePos < queryGraph.queryNodes.size(); ++nodePos) {
        auto subqueryGraph = SubqueryGraph(queryGraph);
        subqueryGraph.addQueryNode(nodePos);
        auto plan = make_unique<LogicalPlan>();
        appendLogicalScan(nodePos, *plan);
        for (auto& expression :
            getNewMatchedWhereExpressions(SubqueryGraph(queryGraph) /* empty subgraph */,
                subqueryGraph, whereClauseAndIncludedVariables)) {
            appendFilter(expression, *plan);
        }
        subgraphPlanTable->addSubgraphPlan(subqueryGraph, move(plan));
    }
}

void Enumerator::enumerateNextNumQueryRel(uint32_t& numEnumeratedQueryRels) {
    numEnumeratedQueryRels++;
    enumerateExtend(numEnumeratedQueryRels);
    if constexpr (ENABLE_HASH_JOIN_ENUMERATION) {
        if (numEnumeratedQueryRels >= 4) {
            enumerateHashJoin(numEnumeratedQueryRels);
        }
    }
}

void Enumerator::enumerateExtend(uint32_t nextNumEnumeratedQueryRels) {
    auto& subgraphPlansMap = subgraphPlanTable->subgraphPlans[nextNumEnumeratedQueryRels - 1];
    for (auto& [prevSubgraph, prevPlans] : subgraphPlansMap) {
        auto connectedQueryRelsWithDirection =
            queryGraph.getConnectedQueryRelsWithDirection(prevSubgraph);
        for (auto& [relPos, isSrcConnected, isDstConnected] : connectedQueryRelsWithDirection) {
            for (auto& prevPlan : prevPlans) {
                auto newSubgraph = prevSubgraph;
                newSubgraph.addQueryRel(relPos);
                if (isSrcConnected && isDstConnected) {
                    throw invalid_argument("Logical intersect is not yet supported.");
                }
                auto plan = prevPlan->copy();
                appendLogicalExtend(relPos, isSrcConnected ? FWD : BWD, *plan);
                for (auto& expression : getNewMatchedWhereExpressions(
                         prevSubgraph, newSubgraph, whereClauseAndIncludedVariables)) {
                    appendFilter(expression, *plan);
                }
                subgraphPlanTable->addSubgraphPlan(newSubgraph, move(plan));
            }
        }
    }
}

void Enumerator::enumerateHashJoin(uint32_t nextNumEnumeratedQueryRels) {
    for (auto leftSize = nextNumEnumeratedQueryRels - 2;
         leftSize >= ceil(nextNumEnumeratedQueryRels / 2.0); --leftSize) {
        auto& subgraphPlansMap = subgraphPlanTable->subgraphPlans[leftSize];
        for (auto& [leftSubgraph, leftPlans] : subgraphPlansMap) {
            auto rightSubgraphAndJoinNodePairs = queryGraph.getSingleNodeJoiningSubgraph(
                leftSubgraph, nextNumEnumeratedQueryRels - leftSize);
            for (auto& [rightSubgraph, joinNodePos] : rightSubgraphAndJoinNodePairs) {
                auto& rightPlans = subgraphPlanTable->getSubgraphPlans(rightSubgraph);
                auto newSubgraph = leftSubgraph;
                newSubgraph.addSubqueryGraph(rightSubgraph);
                auto expressionsToFilter = getNewMatchedWhereExpressions(
                    leftSubgraph, rightSubgraph, newSubgraph, whereClauseAndIncludedVariables);
                for (auto& leftPlan : leftPlans) {
                    for (auto& rightPlan : rightPlans) {
                        auto plan = leftPlan->copy();
                        appendLogicalHashJoin(joinNodePos, *rightPlan, *plan);
                        for (auto& expression : expressionsToFilter) {
                            appendFilter(expression, *plan);
                        }
                        subgraphPlanTable->addSubgraphPlan(newSubgraph, move(plan));
                        // flip build and probe side to get another HashJoin plan
                        if (leftSize != nextNumEnumeratedQueryRels - leftSize) {
                            auto planFilpped = rightPlan->copy();
                            appendLogicalHashJoin(joinNodePos, *leftPlan, *planFilpped);
                            for (auto& expression : expressionsToFilter) {
                                appendFilter(expression, *planFilpped);
                            }
                            subgraphPlanTable->addSubgraphPlan(newSubgraph, move(planFilpped));
                        }
                    }
                }
            }
        }
    }
}

void Enumerator::appendLogicalScan(uint32_t queryNodePos, LogicalPlan& plan) {
    auto& queryNode = *queryGraph.queryNodes[queryNodePos];
    if (ANY_LABEL == queryNode.label) {
        throw invalid_argument("Match any label is not yet supported in LogicalScanNodeID.");
    }
    auto scan = make_shared<LogicalScanNodeID>(queryNode.name, queryNode.label);
    plan.schema->nameOperatorMap.insert({queryNode.name, scan.get()});
    plan.appendOperator(scan);
}

void Enumerator::appendLogicalExtend(uint32_t queryRelPos, Direction direction, LogicalPlan& plan) {
    auto& queryRel = *queryGraph.queryRels[queryRelPos];
    if (ANY_LABEL == queryRel.srcNode->label && ANY_LABEL == queryRel.dstNode->label &&
        ANY_LABEL == queryRel.label) {
        throw invalid_argument("Match any label is not yet supported in LogicalExtend");
    }
    auto extend = make_shared<LogicalExtend>(queryRel, direction, plan.lastOperator);
    auto nbrNodeName = FWD == direction ? queryRel.getSrcNodeName() : queryRel.getDstNodeName();
    plan.schema->addOperator(nbrNodeName, extend.get());
    plan.schema->addOperator(queryRel.name, extend.get());
    plan.appendOperator(extend);
}

void Enumerator::appendLogicalHashJoin(
    uint32_t joinNodePos, const LogicalPlan& planToJoin, LogicalPlan& plan) {
    auto joinNodeName = queryGraph.queryNodes[joinNodePos]->name;
    auto hashJoin =
        make_shared<LogicalHashJoin>(joinNodeName, plan.lastOperator, planToJoin.lastOperator);
    for (auto& [name, op] : planToJoin.schema->nameOperatorMap) {
        if (!plan.schema->containsName(name)) {
            plan.schema->addOperator(name, op);
        }
    }
    plan.appendOperator(hashJoin);
}

void Enumerator::appendFilter(shared_ptr<LogicalExpression> expression, LogicalPlan& plan) {
    appendNecessaryScans(expression, plan);
    auto filter = make_shared<LogicalFilter>(expression, plan.lastOperator);
    plan.appendOperator(filter);
}

void Enumerator::appendProjection(LogicalPlan& plan) {
    for (auto& expression : returnClause) {
        appendNecessaryScans(expression, plan);
    }
    auto projection = make_shared<LogicalProjection>(returnClause, plan.lastOperator);
    plan.appendOperator(projection);
}

void Enumerator::appendNecessaryScans(shared_ptr<LogicalExpression> expression, LogicalPlan& plan) {
    for (auto& includedPropertyName : expression->getIncludedProperties()) {
        if (plan.schema->containsName(includedPropertyName)) {
            continue;
        }
        auto [nodeOrRelName, propertyName] = splitVariableAndPropertyName(includedPropertyName);
        queryGraph.containsQueryNode(nodeOrRelName) ?
            appendScanNodeProperty(nodeOrRelName, propertyName, plan) :
            appendScanRelProperty(nodeOrRelName, propertyName, plan);
    }
}

void Enumerator::appendScanNodeProperty(
    const string& nodeName, const string& propertyName, LogicalPlan& plan) {
    auto queryNode = queryGraph.getQueryNode(nodeName);
    auto scanProperty = make_shared<LogicalScanNodeProperty>(
        queryNode->name, queryNode->label, propertyName, plan.lastOperator);
    plan.schema->addOperator(nodeName + "." + propertyName, scanProperty.get());
    plan.appendOperator(scanProperty);
}

void Enumerator::appendScanRelProperty(
    const string& relName, const string& propertyName, LogicalPlan& plan) {
    auto extend = (LogicalExtend*)plan.schema->getOperator(relName);
    auto scanProperty = make_shared<LogicalScanRelProperty>(
        *queryGraph.getQueryRel(relName), extend->direction, propertyName, plan.lastOperator);
    plan.schema->addOperator(relName + "." + propertyName, scanProperty.get());
    plan.appendOperator(scanProperty);
}

vector<shared_ptr<LogicalExpression>> getNewMatchedWhereExpressions(
    const SubqueryGraph& prevSubgraph, const SubqueryGraph& newSubgraph,
    vector<pair<shared_ptr<LogicalExpression>, unordered_set<string>>>&
        expressionsAndIncludedVariables) {
    vector<shared_ptr<LogicalExpression>> expressions;
    for (auto& [expression, includedVariables] : expressionsAndIncludedVariables) {
        if (newSubgraph.containAllVars(includedVariables) &&
            !prevSubgraph.containAllVars(includedVariables)) {
            expressions.push_back(expression);
        }
    }
    return expressions;
}

vector<shared_ptr<LogicalExpression>> getNewMatchedWhereExpressions(
    const SubqueryGraph& prevLeftSubgraph, const SubqueryGraph& prevRightSubgraph,
    const SubqueryGraph& newSubgraph,
    vector<pair<shared_ptr<LogicalExpression>, unordered_set<string>>>&
        expressionsAndIncludedVariables) {
    vector<shared_ptr<LogicalExpression>> expressions;
    for (auto& [expression, includedVariables] : expressionsAndIncludedVariables) {
        if (newSubgraph.containAllVars(includedVariables) &&
            !prevLeftSubgraph.containAllVars(includedVariables) &&
            !prevRightSubgraph.containAllVars(includedVariables)) {
            expressions.push_back(expression);
        }
    }
    return expressions;
}

pair<string, string> splitVariableAndPropertyName(const string& name) {
    auto splitPos = name.find('.');
    return make_pair(name.substr(0, splitPos), name.substr(splitPos + 1));
}

} // namespace planner
} // namespace graphflow
