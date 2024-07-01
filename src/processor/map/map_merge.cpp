#include "planner/operator/persistent/logical_merge.h"
#include "processor/operator/persistent/merge.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapMerge(planner::LogicalOperator* logicalOperator) {
    auto& logicalMerge = logicalOperator->constCast<LogicalMerge>();
    auto outSchema = logicalMerge.getSchema();
    auto inSchema = logicalMerge.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto existenceMarkPos = getDataPos(*logicalMerge.getExistenceMark(), *inSchema);
    auto distinctMarkPos = DataPos();
    if (logicalMerge.hasDistinctMark()) {
        distinctMarkPos = getDataPos(*logicalMerge.getDistinctMark(), *inSchema);
    }
    std::vector<NodeInsertExecutor> nodeInsertExecutors;
    for (auto& info : logicalMerge.getInsertNodeInfos()) {
        nodeInsertExecutors.push_back(getNodeInsertExecutor(&info, *inSchema, *outSchema)->copy());
    }
    std::vector<RelInsertExecutor> relInsertExecutors;
    for (auto& info : logicalMerge.getInsertRelInfos()) {
        relInsertExecutors.push_back(getRelInsertExecutor(&info, *inSchema, *outSchema)->copy());
    }
    std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors;
    for (auto& info : logicalMerge.getOnCreateSetNodeInfos()) {
        onCreateNodeSetExecutors.push_back(getNodeSetExecutor(info, *inSchema));
    }
    std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors;
    for (auto& info : logicalMerge.getOnCreateSetRelInfos()) {
        onCreateRelSetExecutors.push_back(getRelSetExecutor(info, *inSchema));
    }
    std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors;
    for (auto& info : logicalMerge.getOnMatchSetNodeInfos()) {
        onMatchNodeSetExecutors.push_back(getNodeSetExecutor(info, *inSchema));
    }
    std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors;
    for (auto& info : logicalMerge.getOnMatchSetRelInfos()) {
        onMatchRelSetExecutors.push_back(getRelSetExecutor(info, *inSchema));
    }
    auto printInfo = std::make_unique<OPPrintInfo>(logicalMerge.getExpressionsForPrinting());
    return std::make_unique<Merge>(existenceMarkPos, distinctMarkPos,
        std::move(nodeInsertExecutors), std::move(relInsertExecutors),
        std::move(onCreateNodeSetExecutors), std::move(onCreateRelSetExecutors),
        std::move(onMatchNodeSetExecutors), std::move(onMatchRelSetExecutors),
        std::move(prevOperator), getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
