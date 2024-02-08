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
    auto exportDatabase = (LogicalExportDatabase*)logicalOperator;
    auto filePath = exportDatabase->getFilePath();
    auto tableCyphers = exportDatabase->getTableCyphers();
    auto tableNames = exportDatabase->getTableNames();
    auto macroCyphers = exportDatabase->getMacroCyphers();
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    for (auto childCopyTo : exportDatabase->getChildren()) {
        auto childPhysicalOperator = mapOperator(childCopyTo.get());
        children.push_back(std::move(childPhysicalOperator));
    }
    std::unique_ptr<ResultSetDescriptor> resultSetDescriptor;
    return std::make_unique<ExportDB>(filePath, std::move(tableNames), std::move(tableCyphers),
        std::move(macroCyphers), getOperatorID(), exportDatabase->getExpressionsForPrinting(),
        std::move(children));
}

} // namespace processor
} // namespace kuzu
