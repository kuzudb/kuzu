#include "planner/operator/logical_partitioner.h"
#include "processor/operator/partitioner.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapPartitioner(LogicalOperator* logicalOperator) {
    auto logicalPartitioner = reinterpret_cast<LogicalPartitioner*>(logicalOperator);
    auto prevOperator = mapOperator(logicalPartitioner->getChild(0).get());
    auto outFSchema = logicalPartitioner->getSchema();
    std::vector<std::unique_ptr<PartitioningInfo>> infos;
    infos.reserve(logicalPartitioner->getNumInfos());
    for (auto i = 0u; i < logicalPartitioner->getNumInfos(); i++) {
        auto logicalInfo = logicalPartitioner->getInfo(i);
        infos.push_back(std::make_unique<PartitioningInfo>(
            DataPos{outFSchema->getExpressionPos(*logicalInfo->key)},
            getExpressionsDataPos(logicalInfo->payloads, *outFSchema),
            PartitionerFunctions::partitionRelData));
    }
    auto sharedState = std::make_shared<PartitionerSharedState>();
    return std::make_unique<Partitioner>(std::make_unique<ResultSetDescriptor>(outFSchema),
        std::move(infos), std::move(sharedState), std::move(prevOperator), getOperatorID(),
        logicalPartitioner->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
