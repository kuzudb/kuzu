#include "planner/operator/sip/logical_property_collector.h"
#include "processor/operator/gds_call.h"
#include "processor/operator/property_collector.h"
#include "processor/operator/scan/scan_node_table.h"
#include "processor/operator/semi_masker.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapPropertyCollector(
    LogicalOperator* logicalOperator) {
    const auto& propertyCollector = logicalOperator->cast<LogicalPropertyCollector>();
    const auto inSchema = propertyCollector.getChild(0)->getSchema();
    //auto phy = logicalOpToPhysicalOpMap.at(propertyCollector.getOperator());
    PhysicalOperator* physicalOp = nullptr;
    // todo: fix here
    for (auto& [lop, pop] : logicalOpToPhysicalOpMap) {
        if (lop->getOperatorType() == LogicalOperatorType::GDS_CALL) {
            physicalOp = pop;
            break;
        }
    }
    KU_ASSERT(physicalOp->getOperatorType() == PhysicalOperatorType::TABLE_FUNCTION_CALL);
    KU_ASSERT(physicalOp->getChild(0)->getOperatorType() == PhysicalOperatorType::GDS_CALL);
    auto sharedState = std::make_shared<PropertyCollectorSharedState>();
    auto gds = physicalOp->getChild(0)->ptrCast<
        GDSCall>();
    gds->setNodeProperty(&sharedState->nodeProperty);
    auto nodeIDPos = DataPos(inSchema->getExpressionPos(*propertyCollector.getNodeID()));
    auto propPos = DataPos(inSchema->getExpressionPos(*propertyCollector.getProperty()));
    auto info = PropertyCollectorInfo{nodeIDPos, propPos};
    auto printInfo = std::make_unique<PropertyCollectorPrintInfo>(
        PhysicalOperatorUtils::operatorTypeToString(physicalOp->getOperatorType()));
    return std::make_unique<PropertyCollector>(info, sharedState,
        mapOperator(logicalOperator->getChild(0).get()), getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
