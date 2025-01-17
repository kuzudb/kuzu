#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "function/table/hnsw/hnsw_index_functions.h"
#include "processor/execution_context.h"
#include "storage/index/hnsw_index_utils.h"
#include "storage/index/index_utils.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

CreateHNSWSharedState::CreateHNSWSharedState(const CreateHNSWIndexBindData& bindData)
    : SimpleTableFuncSharedState{bindData.maxOffset}, name{bindData.indexName},
      nodeTable{bindData.context->getStorageManager()
                    ->getTable(bindData.tableEntry->getTableID())
                    ->cast<storage::NodeTable>()},
      numNodes{bindData.numNodes}, bindData{&bindData} {
    hnswIndex = std::make_unique<storage::InMemHNSWIndex>(bindData.context, nodeTable,
        bindData.columnID, bindData.config.copy());
    partitionerSharedState = std::make_shared<storage::HNSWIndexPartitionerSharedState>(
        *bindData.context->getMemoryManager());
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    storage::IndexUtils::validateAutoTransaction(*context, CreateHNSWIndexFunction::name);
    const auto indexName = input->getLiteralVal<std::string>(0);
    const auto tableName = input->getLiteralVal<std::string>(1);
    const auto columnName = input->getLiteralVal<std::string>(2);
    auto tableEntry = storage::IndexUtils::bindTable(*context, tableName, indexName,
        storage::IndexOperation::CREATE);
    const auto tableID = tableEntry->getTableID();
    storage::HNSWIndexUtils::validateColumnType(*tableEntry, columnName);
    const auto& table = context->getStorageManager()->getTable(tableID)->cast<storage::NodeTable>();
    auto columnID = tableEntry->getColumnID(columnName);
    auto config = storage::HNSWIndexConfig{input->optionalParams};
    auto numNodes = table.getStats(context->getTransaction()).getTableCard();
    auto maxOffset = numNodes > 0 ? numNodes - 1 : 0;
    return std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry, columnID,
        numNodes, maxOffset, std::move(config));
}

static std::unique_ptr<TableFuncSharedState> initCreateHNSWSharedState(
    const TableFunctionInitInput& input) {
    const auto bindData = input.bindData->constPtrCast<CreateHNSWIndexBindData>();
    return std::make_unique<CreateHNSWSharedState>(*bindData);
}

static std::unique_ptr<TableFuncLocalState> initCreateHNSWLocalState(
    const TableFunctionInitInput& input, TableFuncSharedState*, storage::MemoryManager*) {
    const auto bindData = input.bindData->constPtrCast<CreateHNSWIndexBindData>();
    return std::make_unique<CreateHNSWLocalState>(bindData->maxOffset + 1);
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto& context = *input.context->clientContext;
    const auto sharedState = input.sharedState->ptrCast<CreateHNSWSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (morsel.isInvalid()) {
        return 0;
    }
    const auto& hnswIndex = sharedState->hnswIndex;
    for (auto i = morsel.startOffset; i <= morsel.endOffset; i++) {
        hnswIndex->insert(i, context.getTransaction(),
            input.localState->ptrCast<CreateHNSWLocalState>()->upperVisited,
            input.localState->ptrCast<CreateHNSWLocalState>()->lowerVisited);
    }
    sharedState->numNodesInserted.fetch_add(morsel.endOffset - morsel.startOffset);
    return morsel.endOffset - morsel.startOffset;
}

static void finalizeFunc(const processor::ExecutionContext* context,
    TableFuncSharedState* sharedState) {
    auto clientContext = context->clientContext;
    auto transaction = clientContext->getTransaction();
    const auto hnswSharedState = sharedState->ptrCast<CreateHNSWSharedState>();
    const auto index = hnswSharedState->hnswIndex.get();
    index->shrink(transaction);
    index->finalize(*clientContext->getMemoryManager(), *hnswSharedState->partitionerSharedState);

    const auto bindData = hnswSharedState->bindData->constPtrCast<CreateHNSWIndexBindData>();
    const auto catalog = clientContext->getCatalog();
    auto upperRelTableID =
        catalog
            ->getTableCatalogEntry(transaction,
                storage::HNSWIndexUtils::getUpperGraphTableName(bindData->indexName))
            ->getTableID();
    auto lowerRelTableID =
        catalog
            ->getTableCatalogEntry(transaction,
                storage::HNSWIndexUtils::getLowerGraphTableName(bindData->indexName))
            ->getTableID();
    auto auxInfo = std::make_unique<catalog::HNSWIndexAuxInfo>(upperRelTableID, lowerRelTableID,
        hnswSharedState->nodeTable.getColumn(bindData->columnID).getName(),
        index->getUpperEntryPoint(), index->getLowerEntryPoint(), bindData->config.copy());
    auto indexEntry =
        std::make_unique<catalog::IndexCatalogEntry>(catalog::HNSWIndexCatalogEntry::TYPE_NAME,
            bindData->tableEntry->getTableID(), bindData->indexName, std::move(auxInfo));
    catalog->createIndex(transaction, std::move(indexEntry));
}

static std::string createHNSWIndexTables(main::ClientContext& context,
    const TableFuncBindData& bindData) {
    context.setToUseInternalCatalogEntry();
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

static double progressFunc(TableFuncSharedState* sharedState) {
    const auto hnswSharedState = sharedState->ptrCast<CreateHNSWSharedState>();
    const auto numNodesInserted = hnswSharedState->numNodesInserted.load();
    if (hnswSharedState->numNodes == 0) {
        return 1.0;
    }
    if (numNodesInserted == 0) {
        return 0.0;
    }
    return static_cast<double>(numNodesInserted) / hnswSharedState->numNodes;
}

function_set CreateHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes = {LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, inputTypes);
    func->tableFunc = tableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = initCreateHNSWSharedState;
    func->initLocalStateFunc = initCreateHNSWLocalState;
    func->progressFunc = progressFunc;
    func->finalizeFunc = finalizeFunc;
    func->rewriteFunc = createHNSWIndexTables;
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
