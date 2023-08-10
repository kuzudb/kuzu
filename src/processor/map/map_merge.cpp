#include "planner/logical_plan/persistent/logical_merge.h"
#include "processor/operator/persistent/merge.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapMerge(planner::LogicalOperator* logicalOperator) {
    auto logicalMerge = (LogicalMerge*)logicalOperator;
    auto outSchema = logicalMerge->getSchema();
    auto inSchema = logicalMerge->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto markPos = DataPos(inSchema->getExpressionPos(*logicalMerge->getMark()));
    auto nodesStore = &storageManager.getNodesStore();
    auto relsStore = &storageManager.getRelsStore();
    std::vector<std::unique_ptr<NodeInsertExecutor>> nodeInsertExecutors;
    for (auto& info : logicalMerge->getCreateNodeInfosRef()) {
        nodeInsertExecutors.push_back(
            getNodeInsertExecutor(nodesStore, relsStore, info.get(), *inSchema, *outSchema));
    }
    std::vector<std::unique_ptr<NodeSetExecutor>> nodeSetExecutors;
    for (auto& info : logicalMerge->getCreateNodeSetInfosRef()) {
        nodeSetExecutors.push_back(getNodeSetExecutor(nodesStore, info.get(), *inSchema));
    }
    std::vector<std::unique_ptr<RelInsertExecutor>> relInsertExecutors;
    for (auto& info : logicalMerge->getCreateRelInfosRef()) {
        relInsertExecutors.push_back(
            getRelInsertExecutor(relsStore, info.get(), *inSchema, *outSchema));
    }
    std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors;
    for (auto& info : logicalMerge->getOnCreateSetNodeInfosRef()) {
        onCreateNodeSetExecutors.push_back(getNodeSetExecutor(nodesStore, info.get(), *inSchema));
    }
    std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors;
    for (auto& info : logicalMerge->getOnCreateSetRelInfosRef()) {
        onCreateRelSetExecutors.push_back(getRelSetExecutor(relsStore, info.get(), *inSchema));
    }
    std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors;
    for (auto& info : logicalMerge->getOnMatchSetNodeInfosRef()) {
        onMatchNodeSetExecutors.push_back(getNodeSetExecutor(nodesStore, info.get(), *inSchema));
    }
    std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors;
    for (auto& info : logicalMerge->getOnMatchSetRelInfosRef()) {
        onMatchRelSetExecutors.push_back(getRelSetExecutor(relsStore, info.get(), *inSchema));
    }
    return std::make_unique<Merge>(markPos, std::move(nodeInsertExecutors),
        std::move(nodeSetExecutors), std::move(relInsertExecutors),
        std::move(onCreateNodeSetExecutors), std::move(onCreateRelSetExecutors),
        std::move(onMatchNodeSetExecutors), std::move(onMatchRelSetExecutors),
        std::move(prevOperator), getOperatorID(), logicalMerge->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
