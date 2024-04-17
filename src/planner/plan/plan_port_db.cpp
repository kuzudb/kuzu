#include "binder/bound_export_database.h"
#include "binder/bound_import_database.h"
#include "common/string_utils.h"
#include "planner/operator/persistent/logical_copy_to.h"
#include "planner/operator/simple/logical_export_db.h"
#include "planner/operator/simple/logical_import_db.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::planExportDatabase(const BoundStatement& statement) {
    auto& boundExportDatabase =
        ku_dynamic_cast<const BoundStatement&, const BoundExportDatabase&>(statement);
    auto filePath = boundExportDatabase.getFilePath();
    auto fileType = boundExportDatabase.getFileType();
    auto exportData = boundExportDatabase.getExportData();
    auto logicalOperators = std::vector<std::shared_ptr<LogicalOperator>>();
    auto plan = std::make_unique<LogicalPlan>();
    auto fileTypeStr = FileTypeUtils::toString(fileType);
    StringUtils::toLower(fileTypeStr);
    auto copyToSuffix = "." + fileTypeStr;
    for (auto& exportTableData : *exportData) {
        auto regularQuery = exportTableData.getRegularQuery();
        KU_ASSERT(regularQuery->getStatementType() == StatementType::QUERY);
        auto tablePlan = getBestPlan(*regularQuery);
        auto copyTo = std::make_shared<LogicalCopyTo>(filePath + "/" + exportTableData.tableName +
                                                          copyToSuffix,
            fileType, exportTableData.columnNames, exportTableData.getColumnTypesRef(),
            boundExportDatabase.getCopyOption(), tablePlan->getLastOperator());
        logicalOperators.push_back(std::move(copyTo));
    }
    auto exportDatabase =
        make_shared<LogicalExportDatabase>(boundExportDatabase.getBoundFileInfo()->copy(),
            statement.getStatementResult()->getSingleColumnExpr(), std::move(logicalOperators));
    plan->setLastOperator(std::move(exportDatabase));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planImportDatabase(const BoundStatement& statement) {
    auto& boundImportDatabase =
        ku_dynamic_cast<const BoundStatement&, const BoundImportDatabase&>(statement);
    auto query = boundImportDatabase.getQuery();
    auto plan = std::make_unique<LogicalPlan>();
    auto importDatabase = make_shared<LogicalImportDatabase>(query,
        statement.getStatementResult()->getSingleColumnExpr());
    plan->setLastOperator(std::move(importDatabase));
    return plan;
}

} // namespace planner
} // namespace kuzu
