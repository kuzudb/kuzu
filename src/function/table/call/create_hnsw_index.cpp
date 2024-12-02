#include "function/table/call/create_hnsw_index.h"

#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/table/call_functions.h"
#include "processor/execution_context.h"
#include "storage/index/hnsw_index_utils.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace function {

// TODO(Guodong): Should abstract this common logic with fts into index entry binding.
static void validateIndexNotExist(const main::ClientContext& context, common::table_id_t tableID,
    const std::string& indexName) {
    if (context.getCatalog()->containsIndex(context.getTx(), tableID, indexName)) {
        throw common::BinderException{common::stringFormat("Index: {} already exists in table: {}.",
            indexName, context.getCatalog()->getTableName(context.getTx(), tableID))};
    }
}

// TODO(Guodong/Ziyi): Chagne bindFunc input to const *.
// NOLINTNEXTLINE(readability-non-const-parameter)
static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput* input) {
    const auto indexName = input->inputs[0].getValue<std::string>();
    const auto tableName = input->inputs[1].getValue<std::string>();
    const auto columnName = input->inputs[2].getValue<std::string>();
    auto tableEntry = context->getCatalog()->getTableCatalogEntry(context->getTx(), tableName);
    const auto tableID = tableEntry->getTableID();
    validateIndexNotExist(*context, tableID, indexName);
    storage::HNSWIndexUtils::validateTableAndColumnType(*tableEntry, columnName);
    auto& table = context->getStorageManager()->getTable(tableID)->cast<storage::NodeTable>();
    auto columnID = tableEntry->getColumnID(columnName);
    auto boundData = std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry,
        &table, columnID, table.getStats(context->getTx()).getCardinality());
    return boundData;
}

// TODO(Guodong/Ziyi): Chagne initSharedState input to const &.
// NOLINTNEXTLINE(readability-non-const-parameter)
static std::unique_ptr<TableFuncSharedState> initHNSWSharedState(TableFunctionInitInput& input) {
    const auto bindData = input.bindData->constPtrCast<CreateHNSWIndexBindData>();
    auto sharedState = std::make_unique<CreateHNSWSharedState>(*bindData);
    // TODO(Guodong): Find a way to parallel the scan of the initial caching.
    sharedState->hnswIndex->initialize(bindData->context, *bindData->table, bindData->columnID);
    return sharedState;
}

// TODO(Guodong/Ziyi): Chagne tableFunc input to const &.
// NOLINTNEXTLINE(readability-non-const-parameter)
static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput&) {
    const auto bindData = input.bindData->constPtrCast<CreateHNSWIndexBindData>();
    auto& context = *input.context->clientContext;
    const auto sharedState = input.sharedState->ptrCast<CreateHNSWSharedState>();
    // TODO(Guodong): Parallel the insert here.
    const auto& hnswIndex = sharedState->hnswIndex;
    for (auto i = 0u; i <= bindData->maxOffset; i++) {
        hnswIndex->insert(i, context.getTx());
    }

    // This is the non-perallel execution part.
    const auto hnswSharedState = sharedState->ptrCast<CreateHNSWSharedState>();
    hnswIndex->shrink(context.getTx());
    // TODO(Guodong): Parallel the population here.
    // Populate partitioning buffers for index graphs.
    hnswIndex->finalize(*context.getMemoryManager(), *hnswSharedState->partitionerSharedState);

    auto upperRelTableID = context.getCatalog()->getTableID(context.getTx(),
        storage::HNSWIndexUtils::getUpperGraphTableName(bindData->indexName));
    auto lowerRelTableID = context.getCatalog()->getTableID(context.getTx(),
        storage::HNSWIndexUtils::getLowerGraphTableName(bindData->indexName));
    context.getCatalog()->createIndex(context.getTx(),
        std::make_unique<catalog::HNSWIndexCatalogEntry>(bindData->tableEntry->getTableID(),
            bindData->indexName, bindData->table->getColumn(bindData->columnID).getName(),
            upperRelTableID, lowerRelTableID, hnswIndex->getUpperEntryPoint(),
            hnswIndex->getLowerEntryPoint()));
    return 0;
}

static std::string createHNSWIndexTables(main::ClientContext&, const TableFuncBindData& bindData) {
    const auto hnswBindData = bindData.constPtrCast<CreateHNSWIndexBindData>();
    std::string query = "";
    query += common::stringFormat("CREATE REL TABLE {} (FROM {} TO {});",
        storage::HNSWIndexUtils::getUpperGraphTableName(hnswBindData->indexName),
        hnswBindData->tableEntry->getName(), hnswBindData->tableEntry->getName());
    query += common::stringFormat("CREATE REL TABLE {} (FROM {} TO {});",
        storage::HNSWIndexUtils::getLowerGraphTableName(hnswBindData->indexName),
        hnswBindData->tableEntry->getName(), hnswBindData->tableEntry->getName());
    return query;
}

function_set CreateHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes{common::LogicalTypeID::STRING, common::LogicalTypeID::STRING,
        common::LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, tableFunc, bindFunc, initHNSWSharedState,
        initEmptyLocalState, inputTypes);
    func->rewriteFunc = createHNSWIndexTables;
    func->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
