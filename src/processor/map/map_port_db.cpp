#include "planner/operator/persistent/logical_export_db.h"
#include "processor/operator/persistent/export_db.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapExportDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto exportDatabase =
        ku_dynamic_cast<LogicalOperator*, LogicalExportDatabase*>(logicalOperator);
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    for (auto childCopyTo : exportDatabase->getChildren()) {
        auto childPhysicalOperator = mapOperator(childCopyTo.get());
        children.push_back(std::move(childPhysicalOperator));
    }
    std::unique_ptr<ResultSetDescriptor> resultSetDescriptor;
    return std::make_unique<ExportDB>(exportDatabase->getBoundFileInfo()->copy(), getOperatorID(),
        exportDatabase->getExpressionsForPrinting(), std::move(children));
}

} // namespace processor
} // namespace kuzu
