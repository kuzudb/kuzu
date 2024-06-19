#include "common/data_chunk/data_chunk_collection.h"
#include "common/exception/binder.h"
#include "common/type_utils.h"
#include "common/types/internal_id_t.h"
#include "common/types/interval_t.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "function/table/bind_input.h"
#include "function/table/call_functions.h"
#include "storage/storage_manager.h"
#include "storage/store/list_column.h"
#include "storage/store/node_table.h"
#include "storage/store/string_column.h"
#include "storage/store/struct_column.h"
#include <concepts>

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct StorageInfoLocalState final : public TableFuncLocalState {
    std::unique_ptr<DataChunkCollection> dataChunkCollection;
    idx_t currChunkIdx;

    explicit StorageInfoLocalState(storage::MemoryManager* mm) : currChunkIdx{0} {
        dataChunkCollection = std::make_unique<DataChunkCollection>(mm);
    }
};

struct StorageInfoSharedState final : public CallFuncSharedState {
    std::vector<storage::Column*> columns;

    StorageInfoSharedState(storage::Table* table, common::offset_t maxOffset)
        : CallFuncSharedState{maxOffset} {
        collectColumns(table);
    }

private:
    void collectColumns(storage::Table* table) {
        switch (table->getTableType()) {
        case TableType::NODE: {
            auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(table);
            for (auto columnID = 0u; columnID < nodeTable->getNumColumns(); columnID++) {
                auto collectedColumns = collectColumns(nodeTable->getColumn(columnID));
                columns.insert(columns.end(), collectedColumns.begin(), collectedColumns.end());
            }
        } break;
        case TableType::REL: {
            auto relTable = ku_dynamic_cast<Table*, RelTable*>(table);
            columns.push_back(relTable->getCSROffsetColumn(RelDataDirection::FWD));
            columns.push_back(relTable->getCSRLengthColumn(RelDataDirection::FWD));
            columns.push_back(relTable->getCSROffsetColumn(RelDataDirection::BWD));
            columns.push_back(relTable->getCSRLengthColumn(RelDataDirection::BWD));
            for (auto columnID = 0u; columnID < relTable->getNumColumns(); columnID++) {
                auto column = relTable->getColumn(columnID, RelDataDirection::FWD);
                auto collectedColumns = collectColumns(column);
                columns.insert(columns.end(), collectedColumns.begin(), collectedColumns.end());
                column = relTable->getColumn(columnID, RelDataDirection::BWD);
                collectedColumns = collectColumns(column);
                columns.insert(columns.end(), collectedColumns.begin(), collectedColumns.end());
            }
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    }
    static std::vector<storage::Column*> collectColumns(storage::Column* column) {
        std::vector<Column*> result;
        result.push_back(column);
        if (column->getNullColumn()) {
            result.push_back(column->getNullColumn());
        }
        switch (column->getDataType().getPhysicalType()) {
        case PhysicalTypeID::STRUCT: {
            auto structColumn = ku_dynamic_cast<Column*, StructColumn*>(column);
            auto numChildren = StructType::getNumFields(structColumn->getDataType());
            for (auto i = 0u; i < numChildren; i++) {
                auto childColumn = structColumn->getChild(i);
                auto subColumns = collectColumns(childColumn);
                result.insert(result.end(), subColumns.begin(), subColumns.end());
            }
        } break;
        case PhysicalTypeID::STRING: {
            auto stringColumn = ku_dynamic_cast<Column*, StringColumn*>(column);
            auto& dictionary = stringColumn->getDictionary();
            result.push_back(dictionary.getDataColumn());
            result.push_back(dictionary.getOffsetColumn());
        } break;
        case PhysicalTypeID::ARRAY:
        case PhysicalTypeID::LIST: {
            auto listColumn = ku_dynamic_cast<Column*, ListColumn*>(column);
            result.push_back(listColumn->getDataColumn());
        } break;
        default: {
            // DO NOTHING.
        }
        }
        return result;
    }
};

struct StorageInfoBindData final : public CallTableFuncBindData {
    TableCatalogEntry* tableEntry;
    storage::Table* table;
    ClientContext* context;

    StorageInfoBindData(std::vector<LogicalType> columnTypes, std::vector<std::string> columnNames,
        TableCatalogEntry* tableEntry, storage::Table* table, ClientContext* context)
        : CallTableFuncBindData{std::move(columnTypes), columnNames, 1 /*maxOffset*/},
          tableEntry{tableEntry}, table{table}, context{context} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<StorageInfoBindData>(common::LogicalType::copy(columnTypes),
            columnNames, tableEntry, table, context);
    }
};

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput& /*input*/,
    TableFuncSharedState* /*state*/, storage::MemoryManager* mm) {
    return std::make_unique<StorageInfoLocalState>(mm);
}

static std::unique_ptr<TableFuncSharedState> initStorageInfoSharedState(
    TableFunctionInitInput& input) {
    auto storageInfoBindData = input.bindData->constPtrCast<StorageInfoBindData>();
    return std::make_unique<StorageInfoSharedState>(storageInfoBindData->table,
        storageInfoBindData->maxOffset);
}

static void appendColumnChunkStorageInfo(node_group_idx_t nodeGroupIdx,
    const std::string& tableType, const Column* column, DataChunk& outputChunk,
    ClientContext* context) {
    auto vectorPos = outputChunk.state->getSelVector().getSelSize();
    auto metadata = column->getMetadata(nodeGroupIdx, context->getTx()->getType());
    auto columnType = column->getDataType().toString();
    outputChunk.getValueVector(0)->setValue<uint64_t>(vectorPos, nodeGroupIdx);
    outputChunk.getValueVector(1)->setValue(vectorPos, column->getName());
    outputChunk.getValueVector(2)->setValue(vectorPos, columnType);
    outputChunk.getValueVector(3)->setValue(vectorPos, tableType);
    outputChunk.getValueVector(4)->setValue<uint64_t>(vectorPos, metadata.pageIdx);
    outputChunk.getValueVector(5)->setValue<uint64_t>(vectorPos, metadata.numPages);
    outputChunk.getValueVector(6)->setValue<uint64_t>(vectorPos, metadata.numValues);

    auto customToString = [&]<typename T>(T) {
        outputChunk.getValueVector(7)->setValue(vectorPos,
            std::to_string(metadata.compMeta.min.get<T>()));
        outputChunk.getValueVector(8)->setValue(vectorPos,
            std::to_string(metadata.compMeta.min.get<T>()));
    };
    auto physicalType = column->getDataType().getPhysicalType();
    TypeUtils::visit(
        physicalType, [&](ku_string_t) { customToString(uint32_t()); },
        [&](list_entry_t) { customToString(uint64_t()); },
        [&](internalID_t) { customToString(uint64_t()); },
        [&]<typename T>(T)
            requires(std::integral<T> || std::floating_point<T>)
        {
            auto min = metadata.compMeta.min.get<T>();
            auto max = metadata.compMeta.max.get<T>();
            outputChunk.getValueVector(7)->setValue(vectorPos,
                TypeUtils::entryToString(column->getDataType(), (uint8_t*)&min,
                    outputChunk.getValueVector(7).get()));
            outputChunk.getValueVector(8)->setValue(vectorPos,
                TypeUtils::entryToString(column->getDataType(), (uint8_t*)&max,
                    outputChunk.getValueVector(8).get()));
        },
        // Types which don't support statistics.
        // types not supported by TypeUtils::visit can
        // also be ignored since we don't track statistics for them
        [](int128_t) {}, [](struct_entry_t) {}, [](interval_t) {});
    outputChunk.getValueVector(9)->setValue(vectorPos, metadata.compMeta.toString(physicalType));
    outputChunk.state->getSelVectorUnsafe().incrementSelSize();
}

static void appendStorageInfoForColumn(StorageInfoLocalState* localState, std::string tableType,
    const Column* column, DataChunk& outputChunk, ClientContext* context) {
    auto numNodeGroups = column->getNumNodeGroups(context->getTx());
    for (auto nodeGroupIdx = 0u; nodeGroupIdx < numNodeGroups; nodeGroupIdx++) {
        if (outputChunk.state->getSelVector().getSelSize() == DEFAULT_VECTOR_CAPACITY) {
            localState->dataChunkCollection->append(outputChunk);
            outputChunk.resetAuxiliaryBuffer();
            outputChunk.state->getSelVectorUnsafe().setSelSize(0);
        }
        appendColumnChunkStorageInfo(nodeGroupIdx, tableType, column, outputChunk, context);
    }
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto localState =
        ku_dynamic_cast<TableFuncLocalState*, StorageInfoLocalState*>(input.localState);
    auto sharedState = input.sharedState->ptrCast<StorageInfoSharedState>();
    KU_ASSERT(dataChunk.state->getSelVector().isUnfiltered());
    while (true) {
        if (localState->currChunkIdx < localState->dataChunkCollection->getNumChunks()) {
            // Copy from local state chunk.
            auto& chunk = localState->dataChunkCollection->getChunkUnsafe(localState->currChunkIdx);
            auto numValuesToOutput = chunk.state->getSelVector().getSelSize();
            for (auto columnIdx = 0u; columnIdx < dataChunk.getNumValueVectors(); columnIdx++) {
                auto localVector = chunk.getValueVector(columnIdx);
                auto outputVector = dataChunk.getValueVector(columnIdx);
                for (auto i = 0u; i < numValuesToOutput; i++) {
                    outputVector->copyFromVectorData(i, localVector.get(), i);
                }
            }
            dataChunk.state->getSelVectorUnsafe().setToUnfiltered(numValuesToOutput);
            localState->currChunkIdx++;
            return numValuesToOutput;
        }
        auto morsel = input.sharedState->ptrCast<CallFuncSharedState>()->getMorsel();
        if (!morsel.hasMoreToOutput()) {
            return 0;
        }
        auto bindData = input.bindData->constPtrCast<StorageInfoBindData>();
        auto tableEntry = bindData->tableEntry;
        std::string tableType = tableEntry->getTableType() == TableType::NODE ? "NODE" : "REL";
        for (auto columnID = 0u; columnID < sharedState->columns.size(); columnID++) {
            appendStorageInfoForColumn(localState, tableType, sharedState->columns[columnID],
                dataChunk, bindData->context);
        }
        localState->dataChunkCollection->append(dataChunk);
        dataChunk.resetAuxiliaryBuffer();
        dataChunk.state->getSelVectorUnsafe().setSelSize(0);
    }
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    TableFuncBindInput* input) {
    std::vector<std::string> columnNames = {"node_group_id", "column_name", "data_type",
        "table_type", "start_page_idx", "num_pages", "num_values", "min", "max", "compression"};
    std::vector<LogicalType> columnTypes;
    columnTypes.emplace_back(LogicalType::INT64());
    columnTypes.emplace_back(LogicalType::STRING());
    columnTypes.emplace_back(LogicalType::STRING());
    columnTypes.emplace_back(LogicalType::STRING());
    columnTypes.emplace_back(LogicalType::INT64());
    columnTypes.emplace_back(LogicalType::INT64());
    columnTypes.emplace_back(LogicalType::INT64());
    columnTypes.emplace_back(LogicalType::STRING());
    columnTypes.emplace_back(LogicalType::STRING());
    columnTypes.emplace_back(LogicalType::STRING());
    auto tableName = input->inputs[0].getValue<std::string>();
    auto catalog = context->getCatalog();
    if (!catalog->containsTable(context->getTx(), tableName)) {
        throw BinderException{"Table " + tableName + " does not exist!"};
    }
    auto tableID = catalog->getTableID(context->getTx(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
    auto storageManager = context->getStorageManager();
    auto table = storageManager->getTable(tableID);
    return std::make_unique<StorageInfoBindData>(std::move(columnTypes), std::move(columnNames),
        tableEntry, table, context);
}

function_set StorageInfoFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, initStorageInfoSharedState,
            initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
