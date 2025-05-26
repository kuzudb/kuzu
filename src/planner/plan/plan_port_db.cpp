#include "binder/bound_export_database.h"
#include "binder/bound_import_database.h"
#include "catalog/catalog.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"
#include "function/built_in_function_utils.h"
#include "main/client_context.h"
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

LogicalPlan Planner::planExportDatabase(const BoundStatement& statement) {
    auto& boundExportDatabase = statement.constCast<BoundExportDatabase>();
    auto filePath = boundExportDatabase.getFilePath();
    auto fileType = boundExportDatabase.getFileType();
    auto exportData = boundExportDatabase.getExportData();
    auto logicalOperators = std::vector<std::shared_ptr<LogicalOperator>>();
    auto plan = LogicalPlan();
    auto fileTypeStr = FileTypeUtils::toString(fileType);
    StringUtils::toLower(fileTypeStr);
    // TODO(Ziyi): Shouldn't these be done in Binder?
    std::string name = stringFormat("COPY_{}", FileTypeUtils::toString(fileType));
    auto entry =
        clientContext->getCatalog()->getFunctionEntry(clientContext->getTransaction(), name);
    auto func = function::BuiltInFunctionsUtils::matchFunction(name,
        entry->ptrCast<FunctionCatalogEntry>());
    KU_ASSERT(func != nullptr);
    auto exportFunc = *func->constPtrCast<function::ExportFunction>();
    for (auto& exportTableData : *exportData) {
        auto regularQuery = exportTableData.getRegularQuery();
        KU_ASSERT(regularQuery->getStatementType() == StatementType::QUERY);
        auto tablePlan = planStatement(*regularQuery);
        auto path = clientContext->getVFSUnsafe()->joinPath(filePath, exportTableData.fileName);
        function::ExportFuncBindInput bindInput{exportTableData.columnNames, std::move(path),
            boundExportDatabase.getExportOptions()};
        auto copyTo = std::make_shared<LogicalCopyTo>(exportFunc.bind(bindInput), exportFunc,
            tablePlan.getLastOperator());
        logicalOperators.push_back(std::move(copyTo));
    }
    auto exportDatabase =
        std::make_shared<LogicalExportDatabase>(boundExportDatabase.getBoundFileInfo()->copy(),
            statement.getSingleColumnExpr(), std::move(logicalOperators));
    plan.setLastOperator(std::move(exportDatabase));
    return plan;
}

LogicalPlan Planner::planImportDatabase(const BoundStatement& statement) {
    auto& boundImportDatabase = statement.constCast<BoundImportDatabase>();
    auto plan = LogicalPlan();
    auto importDatabase = std::make_shared<LogicalImportDatabase>(boundImportDatabase.getQuery(),
        boundImportDatabase.getIndexQuery(), statement.getSingleColumnExpr());
    plan.setLastOperator(std::move(importDatabase));
    return plan;
}

} // namespace planner
} // namespace kuzu
