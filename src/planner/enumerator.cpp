#include "src/planner/include/enumerator.h"

namespace graphflow {
namespace planner {

static shared_ptr<LogicalExtend> createLogicalExtend(
    const QueryRel& queryRel, bool isFwd, shared_ptr<LogicalOperator> prevOperator);

static shared_ptr<LogicalScan> createLogicalScan(const QueryNode& queryNode);

vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans() {
    vector<unique_ptr<LogicalPlan>> plans;
    if (1 == queryGraph.queryNodes.size()) {
        plans.emplace_back(make_unique<LogicalPlan>(createLogicalScan(*queryGraph.queryNodes[0])));
        return plans;
    }
    auto numEnumeratedQueryRels = 0u;
    enumerateSingleQueryRel(numEnumeratedQueryRels);
    while (numEnumeratedQueryRels < queryGraph.queryRels.size()) {
        enumerateNextNumQueryRel(numEnumeratedQueryRels);
    }
    auto& lastOperators =
        subgraphPlanTable->subgraphPlans[queryGraph.queryRels.size()].begin()->second;
    for (auto& lastOperator : lastOperators) {
        plans.emplace_back(make_unique<LogicalPlan>(lastOperator));
    }
    return plans;
}

void Enumerator::enumerateSingleQueryRel(uint32_t& numEnumeratedQueryRels) {
    numEnumeratedQueryRels++;
    for (auto relIdx = 0u; relIdx < queryGraph.queryRels.size(); ++relIdx) {
        auto subqueryGraph = SubqueryGraph(queryGraph);
        subqueryGraph.addQueryRel(relIdx);
        auto& rel = *queryGraph.queryRels[relIdx];
        auto scanSrc = createLogicalScan(*rel.srcNode);
        auto extendDst = createLogicalExtend(rel, true, scanSrc);
        subgraphPlanTable->addSubgraphPlan(subqueryGraph, extendDst);
        auto scanDst = createLogicalScan(*rel.dstNode);
        auto extendSrc = createLogicalExtend(rel, false, scanDst);
        subgraphPlanTable->addSubgraphPlan(subqueryGraph, extendSrc);
    }
}

void Enumerator::enumerateNextNumQueryRel(uint32_t& numEnumeratedQueryRels) {
    numEnumeratedQueryRels++;
    enumerateExtend(numEnumeratedQueryRels);
    /*if (numEnumeratedQueryRels >= 4) {
        enumerateHashJoin(numEnumeratedQueryRels);
    }*/
}

void Enumerator::enumerateExtend(uint32_t nextNumEnumeratedQueryRels) {
    auto& subgraphPlansMap = subgraphPlanTable->subgraphPlans[nextNumEnumeratedQueryRels - 1];
    for (auto& [subgraph, plans] : subgraphPlansMap) {
        auto connectedQueryRelsWithDirection =
            queryGraph.getConnectedQueryRelsWithDirection(subgraph);
        for (auto& [relIdx, isSrcConnected, isDstConnected] : connectedQueryRelsWithDirection) {
            for (auto& plan : plans) {
                auto newSubgraph = subgraph;
                newSubgraph.addQueryRel(relIdx);
                if (isSrcConnected && isDstConnected) {
                    throw invalid_argument("Logical intersect is not yet supported.");
                }
                auto extend =
                    createLogicalExtend(*queryGraph.queryRels[relIdx], isSrcConnected, plan);
                subgraphPlanTable->addSubgraphPlan(newSubgraph, extend);
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
                        subgraphPlanTable->addSubgraphPlan(newSubgraph,
                            make_shared<LogicalHashJoin>(
                                queryGraph.queryNodes[joinNodePos]->name, leftPlan, rightPlan));
                        // flip build and probe side to get another HashJoin plan
                        if (leftSize != nextNumEnumeratedQueryRels - leftSize) {
                            subgraphPlanTable->addSubgraphPlan(newSubgraph,
                                make_shared<LogicalHashJoin>(
                                    queryGraph.queryNodes[joinNodePos]->name, rightPlan, leftPlan));
                        }
                    }
                }
            }
        }
    }
}

shared_ptr<LogicalExtend> createLogicalExtend(
    const QueryRel& queryRel, bool isFwd, shared_ptr<LogicalOperator> prevOperator) {
    if (ANY_LABEL == queryRel.srcNode->label && ANY_LABEL == queryRel.dstNode->label &&
        ANY_LABEL == queryRel.label) {
        throw invalid_argument("Match any label is not yet supported in LogicalExtend");
    }
    if (isFwd) {
        return make_shared<LogicalExtend>(queryRel.getSrcNodeName(), queryRel.srcNode->label,
            queryRel.getDstNodeName(), queryRel.dstNode->label, queryRel.label, FWD, prevOperator);
    }
    return make_shared<LogicalExtend>(queryRel.getDstNodeName(), queryRel.dstNode->label,
        queryRel.getSrcNodeName(), queryRel.srcNode->label, queryRel.label, BWD, prevOperator);
}

shared_ptr<LogicalScan> createLogicalScan(const QueryNode& queryNode) {
    if (ANY_LABEL == queryNode.label) {
        throw invalid_argument("Match any label is not yet supported in LogicalScan.");
    }
    return make_shared<LogicalScan>(queryNode.name, queryNode.label);
}

} // namespace planner
} // namespace graphflow
