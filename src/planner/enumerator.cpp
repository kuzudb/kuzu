#include "src/planner/include/enumerator.h"

namespace graphflow {
namespace planner {

static shared_ptr<LogicalExtend> createLogicalExtend(
    const QueryRel& queryRel, bool isFwd, shared_ptr<LogicalOperator> prevOperator);

static shared_ptr<LogicalScan> createLogicalScan(const QueryNode& queryNode);

vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans() {
    auto numEnumeratedQueryRels = 0u;
    enumerateSingleQueryRel(numEnumeratedQueryRels);
    while (numEnumeratedQueryRels < queryGraph.numQueryRels()) {
        enumerateNextNumQueryRel(numEnumeratedQueryRels);
    }
    vector<unique_ptr<LogicalPlan>> plans;
    auto& lastOperators =
        subgraphPlanTable->getSubgraphPlans(queryGraph.numQueryRels()).begin()->second;
    for (auto& lastOperator : lastOperators) {
        plans.emplace_back(make_unique<LogicalPlan>(lastOperator));
    }
    return plans;
}

void Enumerator::enumerateSingleQueryRel(uint32_t& numEnumeratedQueryRels) {
    numEnumeratedQueryRels++;
    auto queryRels = queryGraph.getQueryRels();
    for (auto& queryRel : queryRels) {
        unordered_set<string> matchedQueryRels;
        matchedQueryRels.insert(queryRel->getName());
        auto scanSrc = createLogicalScan(*queryRel->getSrcNode());
        auto extendDst = createLogicalExtend(*queryRel, true, scanSrc);
        subgraphPlanTable->addSubgraphPlan(matchedQueryRels, extendDst);
        auto scanDst = createLogicalScan(*queryRel->getDstNode());
        auto extendSrc = createLogicalExtend(*queryRel, false, scanDst);
        subgraphPlanTable->addSubgraphPlan(matchedQueryRels, extendSrc);
    }
}

void Enumerator::enumerateNextNumQueryRel(uint32_t& numEnumeratedQueryRels) {
    numEnumeratedQueryRels++;
    enumerateExtend(numEnumeratedQueryRels);
    if (numEnumeratedQueryRels >= 4) {
        enumerateHashJoin(numEnumeratedQueryRels);
    }
}

void Enumerator::enumerateExtend(uint32_t nextNumEnumeratedQueryRels) {
    auto& prevSubgraphPlansMap =
        subgraphPlanTable->getSubgraphPlans(nextNumEnumeratedQueryRels - 1);
    for (auto& mathchedRelsAndPrevPlans : prevSubgraphPlansMap) {
        auto& matchedRels = mathchedRelsAndPrevPlans.first;
        auto& prevPlans = mathchedRelsAndPrevPlans.second;
        auto connectedQueryRelsWithDirection =
            queryGraph.getConnectedQueryRelsWithDirection(matchedRels);
        for (auto& connectedQueryRelWithDirection : connectedQueryRelsWithDirection) {
            auto connectedQueryRel = connectedQueryRelWithDirection.first;
            auto isFwd = connectedQueryRelWithDirection.second;
            for (auto& prevPlan : prevPlans) {
                auto matchedQueryRels = matchedRels;
                matchedQueryRels.insert(connectedQueryRel->getName());
                auto extend = createLogicalExtend(*connectedQueryRel, isFwd, prevPlan);
                subgraphPlanTable->addSubgraphPlan(matchedQueryRels, extend);
            }
        }
    }
}

void Enumerator::enumerateHashJoin(uint32_t nextNumEnumeratedQueryRels) {
    for (auto leftSize = nextNumEnumeratedQueryRels - 2;
         leftSize >= ceil(nextNumEnumeratedQueryRels / 2.0); --leftSize) {
        auto& subgraphPlansMap = subgraphPlanTable->getSubgraphPlans(leftSize);
        for (auto& leftQueryRelsAndSubgraphPlans : subgraphPlansMap) {
            auto& leftQueryRels = leftQueryRelsAndSubgraphPlans.first;
            auto rightSubgraphAndJoinNodePairs = queryGraph.getSingleNodeJoiningSubgraph(
                leftQueryRels, nextNumEnumeratedQueryRels - leftSize);
            for (auto& rightSubgraphAndJoinNodePair : rightSubgraphAndJoinNodePairs) {
                auto& leftPlans = leftQueryRelsAndSubgraphPlans.second;
                auto& rightQueryRels = rightSubgraphAndJoinNodePair.first.queryRelNames;
                auto& rightPlans = subgraphPlanTable->getSubgraphPlans(rightQueryRels);
                auto matchedQueryRels = leftQueryRels;
                matchedQueryRels.insert(begin(rightQueryRels), end(rightQueryRels));
                for (auto& leftPlan : leftPlans) {
                    for (auto& rightPlan : rightPlans) {
                        subgraphPlanTable->addSubgraphPlan(matchedQueryRels,
                            make_shared<LogicalHashJoin>(
                                rightSubgraphAndJoinNodePair.second, leftPlan, rightPlan));
                        // flip build and probe side to get another HashJoin plan
                        if (leftSize != nextNumEnumeratedQueryRels - leftSize) {
                            subgraphPlanTable->addSubgraphPlan(matchedQueryRels,
                                make_shared<LogicalHashJoin>(
                                    rightSubgraphAndJoinNodePair.second, rightPlan, leftPlan));
                        }
                    }
                }
            }
        }
    }
}

shared_ptr<LogicalExtend> createLogicalExtend(
    const QueryRel& queryRel, bool isFwd, shared_ptr<LogicalOperator> prevOperator) {
    if (ANY_LABEL == queryRel.getSrcNode()->getLabel() &&
        ANY_LABEL == queryRel.getDstNode()->getLabel() && ANY_LABEL == queryRel.getLabel()) {
        throw invalid_argument("Match any label is not yet supported in LogicalExtend");
    }
    if (isFwd) {
        return make_shared<LogicalExtend>(queryRel.getSrcNode()->getName(),
            queryRel.getSrcNode()->getLabel(), queryRel.getDstNode()->getName(),
            queryRel.getDstNode()->getLabel(), queryRel.getLabel(), FWD, prevOperator);
    }
    return make_shared<LogicalExtend>(queryRel.getDstNode()->getName(),
        queryRel.getDstNode()->getLabel(), queryRel.getSrcNode()->getName(),
        queryRel.getSrcNode()->getLabel(), queryRel.getLabel(), BWD, prevOperator);
}

shared_ptr<LogicalScan> createLogicalScan(const QueryNode& queryNode) {
    if (ANY_LABEL == queryNode.getLabel()) {
        throw invalid_argument("Match any label is not yet supported in LogicalScan.");
    }
    return make_shared<LogicalScan>(queryNode.getName(), queryNode.getLabel());
}

} // namespace planner
} // namespace graphflow
