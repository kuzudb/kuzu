#include "planner/operator/persistent/logical_export_db.h"
#include "planner/operator/persistent/logical_import_db.h"
#include "processor/operator/persistent/export_db.h"
#include "processor/operator/persistent/import_db.h"
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

std::unique_ptr<PhysicalOperator> PlanMapper::mapImportDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto importDatabase =
        ku_dynamic_cast<LogicalOperator*, LogicalImportDatabase*>(logicalOperator);
    std::unique_ptr<ResultSetDescriptor> resultSetDescriptor;
    return std::make_unique<ImportDB>(
        importDatabase->getQuery(), getOperatorID(), importDatabase->getExpressionsForPrinting());
}
} // namespace processor
} // namespace kuzu
