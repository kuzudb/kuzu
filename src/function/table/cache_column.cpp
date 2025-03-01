#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/table/bind_data.h"
#include "function/table/table_function.h"
#include "processor/execution_context.h"
#include "storage/local_cached_column.h"
#include "storage/storage_manager.h"
#include "storage/store/list_chunk_data.h"
#include "storage/store/node_table.h"
#include "storage/store/table.h"
#include "transaction/transaction.h"

namespace kuzu::catalog {
class TableCatalogEntry;
}

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct CacheArrayColumnBindData final : TableFuncBindData {
    catalog::TableCatalogEntry* tableEntry;
    property_id_t propertyID;

    CacheArrayColumnBindData(catalog::TableCatalogEntry* tableEntry, property_id_t propertyID)
        : tableEntry{tableEntry}, propertyID{propertyID} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CacheArrayColumnBindData>(tableEntry, propertyID);
    }
};

static void validateArrayColumnType(const catalog::TableCatalogEntry* entry,
    property_id_t propertyID) {
    auto& type = entry->getProperty(propertyID).getType();
    if (type.getLogicalTypeID() != LogicalTypeID::ARRAY) {
        throw BinderException{stringFormat("Column {} is not of the expected type {}.",
            entry->getProperty(propertyID).getName(),
            LogicalTypeUtils::toString(LogicalTypeID::ARRAY))};
    }
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    const auto tableName = input->getLiteralVal<std::string>(0);
    const auto columnName = input->getLiteralVal<std::string>(1);
    binder::Binder::validateTableExistence(*context, tableName);
    const auto tableEntry =
        context->getCatalog()->getTableCatalogEntry(context->getTransaction(), tableName);
    binder::Binder::validateNodeTableType(tableEntry);
    binder::Binder::validateColumnExistence(tableEntry, columnName);
    auto propertyID = tableEntry->getPropertyID(columnName);
    validateArrayColumnType(tableEntry, propertyID);
    return std::make_unique<CacheArrayColumnBindData>(tableEntry, propertyID);
}

struct CacheArrayColumnSharedState final : TableFuncSharedState {
    explicit CacheArrayColumnSharedState(storage::NodeTable& table,
        node_group_idx_t maxNodeGroupIdx, const CacheArrayColumnBindData& bindData)
        : TableFuncSharedState{maxNodeGroupIdx, 1 /*maxMorselSize*/}, table{table} {
        cachedColumn = std::make_unique<storage::CachedColumn>(bindData.tableEntry->getTableID(),
            bindData.propertyID);
        cachedColumn->columnChunks.resize(maxNodeGroupIdx + 1);
    }

    void merge(node_group_idx_t nodeGroupIdx,
        std::unique_ptr<storage::ColumnChunkData> columnChunkData) {
        std::unique_lock lck{mtx};
        KU_ASSERT(cachedColumn->columnChunks.size() > nodeGroupIdx);
        cachedColumn->columnChunks[nodeGroupIdx] = std::move(columnChunkData);
        ++numNodeGroupsCached;
    }

    std::mutex mtx;
    storage::NodeTable& table;
    std::unique_ptr<storage::CachedColumn> cachedColumn;
    std::atomic<node_group_idx_t> numNodeGroupsCached;
};

static std::unique_ptr<TableFuncSharedState> initSharedState(const TableFunctionInitInput& input) {
    const auto bindData = input.bindData->constPtrCast<CacheArrayColumnBindData>();
    auto& table = input.context.getStorageManager()
                      ->getTable(bindData->tableEntry->getTableID())
                      ->cast<storage::NodeTable>();
    auto numCommitedNodeGroups = table.getNumCommittedNodeGroups();
    return std::make_unique<CacheArrayColumnSharedState>(table,
        numCommitedNodeGroups == 0 ? 0 : numCommitedNodeGroups, *bindData);
}

struct CacheArrayColumnLocalState final : TableFuncLocalState {
    CacheArrayColumnLocalState(const main::ClientContext& context, table_id_t tableID,
        column_id_t columnID)
        : dataChunk{2, std::make_shared<DataChunkState>()} {
        auto& table = context.getStorageManager()->getTable(tableID)->cast<storage::NodeTable>();
        dataChunk.insert(0, std::make_shared<ValueVector>(LogicalType::INTERNAL_ID()));
        dataChunk.insert(1,
            std::make_shared<ValueVector>(table.getColumn(columnID).getDataType().copy()));
        std::vector<column_id_t> columnIDs;
        columnIDs.push_back(columnID);
        scanState =
            std::make_unique<storage::NodeTableScanState>(&dataChunk.getValueVectorMutable(0),
                std::vector{&dataChunk.getValueVectorMutable(1)}, dataChunk.state);
        scanState->source = storage::TableScanSource::COMMITTED;
        scanState->setToTable(context.getTransaction(), &table, columnIDs, {});
    }

    DataChunk dataChunk;
    std::unique_ptr<storage::NodeTableScanState> scanState;
};

static std::unique_ptr<TableFuncLocalState> initLocalState(const TableFunctionInitInput& input,
    TableFuncSharedState*, storage::MemoryManager*) {
    const auto bindData = input.bindData->constPtrCast<CacheArrayColumnBindData>();
    auto tableID = bindData->tableEntry->getTableID();
    auto columnID = bindData->tableEntry->getColumnID(bindData->propertyID);
    return std::make_unique<CacheArrayColumnLocalState>(input.context, tableID, columnID);
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto& bindData = input.bindData->cast<CacheArrayColumnBindData>();
    const auto sharedState = input.sharedState->ptrCast<CacheArrayColumnSharedState>();
    auto localState = input.localState->ptrCast<CacheArrayColumnLocalState>();
    const auto morsel = sharedState->getMorsel();
    if (morsel.isInvalid()) {
        return 0;
    }
    auto context = input.context->clientContext;
    auto columnType = bindData.tableEntry->getProperty(bindData.propertyID).getType().copy();
    auto& table = sharedState->table;
    auto& scanState = *localState->scanState;
    for (auto i = morsel.startOffset; i < morsel.endOffset; i++) {
        scanState.nodeGroupIdx = i;
        auto numRows = table.getNumTuplesInNodeGroup(i);
        auto data = storage::ColumnChunkFactory::createColumnChunkData(*context->getMemoryManager(),
            columnType.copy(), false /*enableCompression*/, numRows,
            storage::ResidencyState::IN_MEMORY, true /*hasNullData*/, false /*initializeToZero*/);
        if (columnType.getLogicalTypeID() == LogicalTypeID::ARRAY) {
            auto arrayTypeInfo = columnType.getExtraTypeInfo()->constPtrCast<ArrayTypeInfo>();
            data->cast<storage::ListChunkData>().getDataColumnChunk()->resize(
                numRows * arrayTypeInfo->getNumElements());
        }
        table.initScanState(context->getTransaction(), *localState->scanState);
        while (table.scan(context->getTransaction(), scanState)) {
            data->append(scanState.outputVectors[0], scanState.outState->getSelVector());
        }
        sharedState->merge(i, std::move(data));
    }
    return morsel.endOffset - morsel.startOffset;
}

static double progressFunc(TableFuncSharedState* sharedState) {
    const auto cacheColumnSharedState = sharedState->ptrCast<CacheArrayColumnSharedState>();
    const auto numNodeGroupsCached = cacheColumnSharedState->numNodeGroupsCached.load();
    if (cacheColumnSharedState->maxOffset == 0) {
        return 1.0;
    }
    if (numNodeGroupsCached == 0) {
        return 0.0;
    }
    return static_cast<double>(numNodeGroupsCached) / cacheColumnSharedState->maxOffset;
}

static void finalizeFunc(const processor::ExecutionContext* context,
    TableFuncSharedState* sharedState) {
    auto transaction = context->clientContext->getTransaction();
    auto cacheColumnSharedState = sharedState->ptrCast<CacheArrayColumnSharedState>();
    auto& localCacheManager = transaction->getLocalCacheManager();
    localCacheManager.put(std::move(cacheColumnSharedState->cachedColumn));
}

function_set LocalCacheArrayColumnFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes = {LogicalTypeID::STRING, LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, inputTypes);
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = initSharedState;
    func->initLocalStateFunc = initLocalState;
    func->tableFunc = tableFunc;
    func->finalizeFunc = finalizeFunc;
    func->canParallelFunc = [] { return true; };
    func->progressFunc = progressFunc;
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
