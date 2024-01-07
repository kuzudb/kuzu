#include "common/cast.h"
#include "planner/operator/logical_extension.h"
#include "processor/operator/install_extension.h"
#include "processor/operator/load_extension.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapExtension(
    planner::LogicalOperator* logicalOperator) {
    auto logicalExtension =
        common::ku_dynamic_cast<LogicalOperator*, LogicalExtension*>(logicalOperator);
    switch (logicalExtension->getAction()) {
    case ExtensionAction::INSTALL:
        return std::make_unique<InstallExtension>(logicalExtension->getPath(), getOperatorID(),
            logicalExtension->getExpressionsForPrinting());
    case ExtensionAction::LOAD:
        return std::make_unique<LoadExtension>(logicalExtension->getPath(), getOperatorID(),
            logicalExtension->getExpressionsForPrinting());
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu
