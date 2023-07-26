#include "planner/logical_plan/logical_operator/logical_merge.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/update/merge.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapMerge(planner::LogicalOperator* logicalOperator) {
    auto logicalMerge = (LogicalMerge*)logicalOperator;
    auto outSchema = logicalMerge->getSchema();
    auto inSchema = logicalMerge->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto markPos = DataPos(inSchema->getExpressionPos(*logicalMerge->getMark()));
    std::vector<std::unique_ptr<NodeTableInsertManager>> nodeTableInsertManagers;
    for (auto& info : logicalMerge->getCreateNodeInfosRef()) {
        nodeTableInsertManagers.push_back(
            createNodeTableInsertManager(info.get(), *inSchema, *outSchema));
    }
    std::vector<std::unique_ptr<RelTableInsertManager>> relTableInsertManagers;
    for (auto& info : logicalMerge->getCreateRelInfosRef()) {
        relTableInsertManagers.push_back(createRelTableInsertManager(info.get(), *inSchema));
    }
    return std::make_unique<Merge>(markPos, std::move(nodeTableInsertManagers),
        std::move(relTableInsertManagers), std::move(prevOperator), getOperatorID(),
        logicalMerge->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
