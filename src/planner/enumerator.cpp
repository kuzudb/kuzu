#include "src/planner/include/enumerator.h"

namespace graphflow {
namespace planner {

static unique_ptr<LogicalPlan> appendLogicalHashJoin(
    const string& joinNodeName, const LogicalPlan& buildPlan, const LogicalPlan& probePlan);

static unique_ptr<LogicalPlan> appendLogicalExtend(
    const QueryRel& queryRel, bool isFwd, const LogicalPlan& prevPlan);

static unique_ptr<LogicalPlan> appendLogicalScan(const QueryNode& queryNode);

Enumerator::Enumerator(const BoundSingleQuery& boundSingleQuery)
    : queryGraph{*boundSingleQuery.queryGraph} {
    subgraphPlanTable = make_unique<SubgraphPlanTable>(queryGraph.queryRels.size());
    if (!boundSingleQuery.whereExprsSplitByAND.empty()) {
        for (auto& expr : boundSingleQuery.whereExprsSplitByAND) {
            expressionsAndIncludedVariables.emplace_back(
                make_pair(expr, expr->getIncludedVariables()));
        }
    }
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans() {
    auto numEnumeratedQueryRels = 0u;
    enumerateSingleQueryNode();
    while (numEnumeratedQueryRels < queryGraph.queryRels.size()) {
        enumerateNextNumQueryRel(numEnumeratedQueryRels);
    }
    return move(subgraphPlanTable->subgraphPlans[queryGraph.queryRels.size()].begin()->second);
}

void Enumerator::enumerateSingleQueryNode() {
    for (auto nodeIdx = 0u; nodeIdx < queryGraph.queryNodes.size(); ++nodeIdx) {
        auto subqueryGraph = SubqueryGraph(queryGraph);
        subqueryGraph.addQueryNode(nodeIdx);
        auto& queryNode = *queryGraph.queryNodes[nodeIdx];
        auto plan = appendLogicalScan(queryNode);
        appendFiltersIfPossible(
            SubqueryGraph(queryGraph) /* empty subgraph */, subqueryGraph, *plan);
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
        for (auto& [relIdx, isSrcConnected, isDstConnected] : connectedQueryRelsWithDirection) {
            for (auto& prevPlan : prevPlans) {
                auto newSubgraph = prevSubgraph;
                newSubgraph.addQueryRel(relIdx);
                if (isSrcConnected && isDstConnected) {
                    throw invalid_argument("Logical intersect is not yet supported.");
                }
                auto plan =
                    appendLogicalExtend(*queryGraph.queryRels[relIdx], isSrcConnected, *prevPlan);
                appendFiltersIfPossible(prevSubgraph, newSubgraph, *plan);
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
                for (auto& leftPlan : leftPlans) {
                    for (auto& rightPlan : rightPlans) {
                        auto plan = appendLogicalHashJoin(
                            queryGraph.queryNodes[joinNodePos]->name, *leftPlan, *rightPlan);
                        appendFiltersIfPossible(leftSubgraph, rightSubgraph, newSubgraph, *plan);
                        subgraphPlanTable->addSubgraphPlan(newSubgraph, move(plan));
                        // flip build and probe side to get another HashJoin plan
                        if (leftSize != nextNumEnumeratedQueryRels - leftSize) {
                            auto planFlip = appendLogicalHashJoin(
                                queryGraph.queryNodes[joinNodePos]->name, *rightPlan, *leftPlan);
                            appendFiltersIfPossible(
                                leftSubgraph, rightSubgraph, newSubgraph, *plan);
                            subgraphPlanTable->addSubgraphPlan(newSubgraph, move(planFlip));
                        }
                    }
                }
            }
        }
    }
}

void Enumerator::appendFiltersIfPossible(const SubqueryGraph& leftPrevSubgraph,
    const SubqueryGraph& rightPrevSubgraph, const SubqueryGraph& subgraph, LogicalPlan& plan) {
    for (auto& [expr, dependentVars] : expressionsAndIncludedVariables) {
        if (subgraph.containAllVars(dependentVars) &&
            !leftPrevSubgraph.containAllVars(dependentVars) &&
            !rightPrevSubgraph.containAllVars(dependentVars)) {
            appendFilterAndNecessaryScans(expr, plan);
        }
    }
}

void Enumerator::appendFiltersIfPossible(
    const SubqueryGraph& prevSubgraph, const SubqueryGraph& subgraph, LogicalPlan& plan) {
    for (auto& [expr, dependentVars] : expressionsAndIncludedVariables) {
        if (subgraph.containAllVars(dependentVars) && !prevSubgraph.containAllVars(dependentVars)) {
            appendFilterAndNecessaryScans(expr, plan);
        }
    }
}

void Enumerator::appendFilterAndNecessaryScans(
    shared_ptr<LogicalExpression> expr, LogicalPlan& plan) {
    for (auto& dependentPropertyName : expr->getIncludedProperties()) {
        if (plan.schema->containsOperator(dependentPropertyName)) {
            continue;
        }
        appendScanProperty(dependentPropertyName, plan);
    }
    auto filter = make_shared<LogicalFilter>(expr, plan.lastOperator);
    plan.lastOperator = filter;
}

void Enumerator::appendScanProperty(const string& varAndPropertyName, LogicalPlan& plan) {
    auto varName = varAndPropertyName.substr(0, varAndPropertyName.find('.'));
    auto propertyName = varAndPropertyName.substr(varAndPropertyName.find('.') + 1);
    queryGraph.containsQueryNode(varName) ? appendScanNodeProperty(varName, propertyName, plan) :
                                            appendScanRelProperty(varName, propertyName, plan);
}

void Enumerator::appendScanRelProperty(
    const string& relName, const string& propertyName, LogicalPlan& plan) {
    auto extend = (LogicalExtend*)plan.schema->getOperator(relName);
    auto scanProperty = make_shared<LogicalScanRelProperty>(
        *queryGraph.getQueryRel(relName), extend->direction, propertyName, plan.lastOperator);
    plan.schema->addOperator(relName + "." + propertyName, scanProperty.get());
    plan.lastOperator = scanProperty;
}

void Enumerator::appendScanNodeProperty(
    const string& nodeName, const string& propertyName, LogicalPlan& plan) {
    auto queryNode = queryGraph.getQueryNode(nodeName);
    auto scanProperty = make_shared<LogicalScanNodeProperty>(
        queryNode->name, queryNode->label, propertyName, plan.lastOperator);
    plan.schema->addOperator(nodeName + "." + propertyName, scanProperty.get());
    plan.lastOperator = scanProperty;
}

unique_ptr<LogicalPlan> appendLogicalHashJoin(
    const string& joinNodeName, const LogicalPlan& buildPlan, const LogicalPlan& probePlan) {
    auto hashJoin =
        make_shared<LogicalHashJoin>(joinNodeName, buildPlan.lastOperator, probePlan.lastOperator);
    auto schema = probePlan.schema->copy();
    for (auto& [varName, logicalOperator] : buildPlan.schema->varNameOperatorMap) {
        if (!schema->containsOperator(varName)) {
            schema->addOperator(varName, logicalOperator);
        }
    }
    return make_unique<LogicalPlan>(hashJoin, move(schema));
}

unique_ptr<LogicalPlan> appendLogicalExtend(
    const QueryRel& queryRel, bool isFwd, const LogicalPlan& prevPlan) {
    if (ANY_LABEL == queryRel.srcNode->label && ANY_LABEL == queryRel.dstNode->label &&
        ANY_LABEL == queryRel.label) {
        throw invalid_argument("Match any label is not yet supported in LogicalExtend");
    }
    auto schema = prevPlan.schema->copy();
    auto extend = make_shared<LogicalExtend>(queryRel, isFwd ? FWD : BWD, prevPlan.lastOperator);
    schema->addOperator(
        isFwd ? queryRel.getSrcNodeName() : queryRel.getDstNodeName(), extend.get());
    schema->addOperator(queryRel.name, extend.get());
    return make_unique<LogicalPlan>(extend, move(schema));
}

unique_ptr<LogicalPlan> appendLogicalScan(const QueryNode& queryNode) {
    if (ANY_LABEL == queryNode.label) {
        throw invalid_argument("Match any label is not yet supported in LogicalScanNodeID.");
    }
    auto scan = make_shared<LogicalScanNodeID>(queryNode.name, queryNode.label);
    auto schema = make_unique<Schema>();
    schema->addOperator(queryNode.name, scan.get());
    return make_unique<LogicalPlan>(scan, move(schema));
}

} // namespace planner
} // namespace graphflow
