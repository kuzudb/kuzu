#include "src/planner/include/enumerator.h"

namespace graphflow {
namespace planner {

static shared_ptr<LogicalExtend> createLogicalExtend(
    const QueryRel& queryRel, bool isFwd, shared_ptr<LogicalOperator> prevOperator);

static shared_ptr<LogicalScan> createLogicalScan(const QueryNode& queryNode);

Enumerator::Enumerator(unique_ptr<QueryGraph> queryGraph) {
    maxPlanSize = queryGraph->numQueryRels();
    this->queryGraph = move(queryGraph);
    subgraphPlanTable = make_unique<SubgraphPlanTable>(maxPlanSize);
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans() {
    auto numQueryRelMatched = 0u;
    enumerateForInitialQueryRel(*queryGraph, numQueryRelMatched);
    while (numQueryRelMatched < maxPlanSize) {
        enumerateNextQueryRel(*queryGraph, numQueryRelMatched);
    }
    vector<unique_ptr<LogicalPlan>> plans;
    auto& lastOperators = subgraphPlanTable->getSubgraphPlans(maxPlanSize).begin()->second;
    for (auto& lastOperator : lastOperators) {
        plans.emplace_back(make_unique<LogicalPlan>(lastOperator));
    }
    return plans;
}

void Enumerator::enumerateForInitialQueryRel(
    const QueryGraph& queryGraph, uint& numQueryRelMatched) {
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
    numQueryRelMatched++;
}

void Enumerator::enumerateNextQueryRel(const QueryGraph& queryGraph, uint& numQueryRelMatched) {
    auto& prevSubgraphPlansMap = subgraphPlanTable->getSubgraphPlans(numQueryRelMatched);
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
    numQueryRelMatched++;
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
