#include "common/exception/binder.h"
#include "function/table/hnsw/hnsw_index_functions.h"
#include "storage/index/hnsw_index_utils.h"

namespace kuzu {
namespace function {

static void validateTableAndIndexExists(const main::ClientContext& context,
    const std::string& tableName, const std::string& indexName) {
    if (!context.getCatalog()->containsTable(context.getTx(), tableName)) {
        throw common::BinderException{common::stringFormat("Table {} does not exist.", tableName)};
    }
    const auto tableID = context.getCatalog()->getTableID(context.getTx(), tableName);
    if (!context.getCatalog()->containsIndex(context.getTx(), tableID, indexName)) {
        throw common::BinderException{
            common::stringFormat("Table {} doesn't have an HNSW index named {}.",
                context.getCatalog()->getTableName(context.getTx(), tableID), indexName)};
    }
}

struct DropHNSWIndexBindData final : SimpleTableFuncBindData {
    common::table_id_t tableID;
    std::string indexName;

    DropHNSWIndexBindData(common::table_id_t tableID, std::string indexName)
        : SimpleTableFuncBindData{0}, tableID{tableID}, indexName{std::move(indexName)} {}
};

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    TableFuncBindInput* input) {
    const auto indexName = input->getLiteralVal<std::string>(0);
    const auto tableName = input->getLiteralVal<std::string>(1);
    validateTableAndIndexExists(*context, tableName, indexName);
    const auto tableID = context->getCatalog()->getTableID(context->getTx(), tableName);
    return std::make_unique<DropHNSWIndexBindData>(tableID, indexName);
}

// NOLINTNEXTLINE(readability-non-const-parameter)
static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput&) {
    const auto& context = *input.context->clientContext;
    const auto sharedState = input.sharedState->ptrCast<SimpleTableFuncSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (morsel.isInvalid()) {
        return 0;
    }
    const auto bindData = input.bindData->constPtrCast<DropHNSWIndexBindData>();
    context.getCatalog()->dropIndex(context.getTx(), bindData->tableID, bindData->indexName);
    return 1;
}

static std::string dropHNSWIndexTables(main::ClientContext&, const TableFuncBindData& bindData) {
    const auto dropHNSWIndexBindData = bindData.constPtrCast<DropHNSWIndexBindData>();
    std::string query = "";
    query += common::stringFormat("DROP TABLE {};",
        storage::HNSWIndexUtils::getUpperGraphTableName(dropHNSWIndexBindData->indexName));
    query += common::stringFormat("DROP TABLE {};",
        storage::HNSWIndexUtils::getLowerGraphTableName(dropHNSWIndexBindData->indexName));
    return query;
}

function_set DropHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes = {common::LogicalTypeID::STRING, common::LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState,
        initEmptyLocalState, inputTypes);
    func->rewriteFunc = dropHNSWIndexTables;
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
