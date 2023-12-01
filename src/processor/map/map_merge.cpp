#include "planner/operator/persistent/logical_merge.h"
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
    std::vector<std::unique_ptr<NodeInsertExecutor>> nodeInsertExecutors;
    for (auto& info : logicalMerge->getInsertNodeInfosRef()) {
        nodeInsertExecutors.push_back(getNodeInsertExecutor(info.get(), *inSchema, *outSchema));
    }
    std::vector<std::unique_ptr<RelInsertExecutor>> relInsertExecutors;
    for (auto& info : logicalMerge->getInsertRelInfosRef()) {
        relInsertExecutors.push_back(getRelInsertExecutor(info.get(), *inSchema, *outSchema));
    }
    std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors;
    for (auto& info : logicalMerge->getOnCreateSetNodeInfosRef()) {
        onCreateNodeSetExecutors.push_back(getNodeSetExecutor(info.get(), *inSchema));
    }
    std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors;
    for (auto& info : logicalMerge->getOnCreateSetRelInfosRef()) {
        onCreateRelSetExecutors.push_back(getRelSetExecutor(info.get(), *inSchema));
    }
    std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors;
    for (auto& info : logicalMerge->getOnMatchSetNodeInfosRef()) {
        onMatchNodeSetExecutors.push_back(getNodeSetExecutor(info.get(), *inSchema));
    }
    std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors;
    for (auto& info : logicalMerge->getOnMatchSetRelInfosRef()) {
        onMatchRelSetExecutors.push_back(getRelSetExecutor(info.get(), *inSchema));
    }
    return std::make_unique<Merge>(markPos, std::move(nodeInsertExecutors),
        std::move(relInsertExecutors), std::move(onCreateNodeSetExecutors),
        std::move(onCreateRelSetExecutors), std::move(onMatchNodeSetExecutors),
        std::move(onMatchRelSetExecutors), std::move(prevOperator), getOperatorID(),
        logicalMerge->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
