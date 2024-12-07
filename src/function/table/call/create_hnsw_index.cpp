#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/table/call_functions.h"
#include "function/table/hnsw/hnsw_index_functions.h"
#include "processor/execution_context.h"
#include "storage/index/hnsw_index_utils.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

// TODO(Guodong): Should abstract this common logic with fts into index entry binding.
static void validateIndexNotExist(const main::ClientContext& context, table_id_t tableID,
    const std::string& indexName) {
    if (context.getCatalog()->containsIndex(context.getTx(), tableID, indexName)) {
        throw BinderException{stringFormat("Index: {} already exists in table: {}.", indexName,
            context.getCatalog()->getTableName(context.getTx(), tableID))};
    }
}

// TODO(Guodong/Ziyi): Change bindFunc input to const *.
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
    auto config = storage::HNSWIndexConfig{input->optionalParams};
    auto boundData = std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry,
        &table, columnID, table.getStats(context->getTx()).getCardinality(), std::move(config));
    return boundData;
}

// TODO(Guodong/Ziyi): Change initSharedState input to const &.
// NOLINTNEXTLINE(readability-non-const-parameter)
static std::unique_ptr<TableFuncSharedState> initHNSWSharedState(TableFunctionInitInput& input) {
    const auto bindData = input.bindData->constPtrCast<CreateHNSWIndexBindData>();
    auto sharedState = std::make_unique<CreateHNSWSharedState>(*bindData);
    // TODO(Guodong): Find a way to parallel the scan of the initial caching.
    sharedState->hnswIndex->initialize(bindData->context, *bindData->table, bindData->columnID);
    sharedState->bindData = bindData;
    return sharedState;
}

// TODO(Guodong/Ziyi): Change tableFunc input to const &.
// NOLINTNEXTLINE(readability-non-const-parameter)
static offset_t tableFunc(TableFuncInput& input, TableFuncOutput&) {
    const auto& context = *input.context->clientContext;
    const auto sharedState = input.sharedState->ptrCast<CreateHNSWSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (morsel.isInvalid()) {
        return 0;
    }
    const auto& hnswIndex = sharedState->hnswIndex;
    for (auto i = morsel.startOffset; i < morsel.endOffset; i++) {
        hnswIndex->insert(i, context.getTx());
    }
    return morsel.endOffset - morsel.startOffset;
}

// NOLINTNEXTLINE(readability-non-const-parameter)
static void finalizeFunc(processor::ExecutionContext* context, TableFuncSharedState* sharedState,
    TableFuncLocalState*) {
    // This is the non-perallel execution part.
    const auto hnswSharedState = sharedState->ptrCast<CreateHNSWSharedState>();
    hnswSharedState->hnswIndex->shrink(context->clientContext->getTx());
    // TODO(Guodong): Parallel the population here.
    // Populate partitioning buffers for index graphs.
    hnswSharedState->hnswIndex->finalize(*context->clientContext->getMemoryManager(),
        *hnswSharedState->partitionerSharedState);

    const auto bindData = hnswSharedState->bindData->constPtrCast<CreateHNSWIndexBindData>();
    auto upperRelTableID =
        context->clientContext->getCatalog()->getTableID(context->clientContext->getTx(),
            storage::HNSWIndexUtils::getUpperGraphTableName(bindData->indexName));
    auto lowerRelTableID =
        context->clientContext->getCatalog()->getTableID(context->clientContext->getTx(),
            storage::HNSWIndexUtils::getLowerGraphTableName(bindData->indexName));
    context->clientContext->getCatalog()->createIndex(context->clientContext->getTx(),
        std::make_unique<catalog::HNSWIndexCatalogEntry>(bindData->tableEntry->getTableID(),
            bindData->indexName, bindData->table->getColumn(bindData->columnID).getName(),
            upperRelTableID, lowerRelTableID, hnswSharedState->hnswIndex->getUpperEntryPoint(),
            hnswSharedState->hnswIndex->getLowerEntryPoint(), bindData->config));
}

static std::string createHNSWIndexTables(main::ClientContext&, const TableFuncBindData& bindData) {
    const auto hnswBindData = bindData.constPtrCast<CreateHNSWIndexBindData>();
    std::string query = "";
    query += stringFormat("CREATE REL TABLE {} (FROM {} TO {});",
        storage::HNSWIndexUtils::getUpperGraphTableName(hnswBindData->indexName),
        hnswBindData->tableEntry->getName(), hnswBindData->tableEntry->getName());
    query += stringFormat("CREATE REL TABLE {} (FROM {} TO {});",
        storage::HNSWIndexUtils::getLowerGraphTableName(hnswBindData->indexName),
        hnswBindData->tableEntry->getName(), hnswBindData->tableEntry->getName());
    return query;
}

function_set CreateHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes{LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, tableFunc, bindFunc, initHNSWSharedState,
        initEmptyLocalState, inputTypes, finalizeFunc);
    func->rewriteFunc = createHNSWIndexTables;
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
