#pragma once

#include "catalog/catalog_content.h"
#include "common/data_chunk/data_chunk_collection.h"
#include "common/vector/value_vector.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "main/client_context.h"

namespace kuzu {
namespace storage {
class StorageManager;
class Table;
class Column;
} // namespace storage
namespace function {

struct CallFuncMorsel {
    common::offset_t startOffset;
    common::offset_t endOffset;

    CallFuncMorsel(common::offset_t startOffset, common::offset_t endOffset)
        : startOffset{startOffset}, endOffset{endOffset} {}

    inline bool hasMoreToOutput() const { return startOffset != UINT64_MAX; }

    inline static CallFuncMorsel createInvalidMorsel() {
        return CallFuncMorsel{UINT64_MAX, UINT64_MAX};
    }
};

struct CallFuncSharedState : public TableFuncSharedState {
    common::offset_t maxOffset;
    common::offset_t curOffset;
    std::mutex mtx;

    explicit CallFuncSharedState(common::offset_t maxOffset) : maxOffset{maxOffset}, curOffset{0} {}

    CallFuncMorsel getMorsel();
};

struct CallTableFuncBindData : public TableFuncBindData {
    common::offset_t maxOffset;

    CallTableFuncBindData(std::vector<std::unique_ptr<common::LogicalType>> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : TableFuncBindData{std::move(returnTypes), std::move(returnColumnNames)}, maxOffset{
                                                                                       maxOffset} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<CallTableFuncBindData>(
            common::LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

struct CallFunction {
    static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input);
};

struct CurrentSettingBindData : public CallTableFuncBindData {
    std::string result;

    CurrentSettingBindData(std::string result,
        std::vector<std::unique_ptr<common::LogicalType>> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          result{std::move(result)} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<CurrentSettingBindData>(
            result, common::LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

struct CurrentSettingFunction : public CallFunction {
    static function_set getFunctionSet();

    static void tableFunc(TableFunctionInput& data, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
        TableFuncBindInput* input, catalog::Catalog* /*catalog*/,
        storage::StorageManager* /*storageManager*/);
};

struct DBVersionFunction : public CallFunction {
    static function_set getFunctionSet();

    static void tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        TableFuncBindInput* /*input*/, catalog::Catalog* /*catalog*/,
        storage::StorageManager* /*storageManager*/);
};

struct ShowTablesBindData : public CallTableFuncBindData {
    std::vector<catalog::TableSchema*> tables;

    ShowTablesBindData(std::vector<catalog::TableSchema*> tables,
        std::vector<std::unique_ptr<common::LogicalType>> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tables{std::move(tables)} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<ShowTablesBindData>(
            tables, common::LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

struct ShowTablesFunction : public CallFunction {
    static function_set getFunctionSet();

    static void tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        TableFuncBindInput* /*input*/, catalog::Catalog* catalog,
        storage::StorageManager* /*storageManager*/);
};

struct TableInfoBindData : public CallTableFuncBindData {
    catalog::TableSchema* tableSchema;

    TableInfoBindData(catalog::TableSchema* tableSchema,
        std::vector<std::unique_ptr<common::LogicalType>> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tableSchema{tableSchema} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<TableInfoBindData>(
            tableSchema, common::LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

struct TableInfoFunction : public CallFunction {
    static function_set getFunctionSet();

    static void tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        TableFuncBindInput* input, catalog::Catalog* catalog,
        storage::StorageManager* /*storageManager*/);
};

struct ShowConnectionBindData : public TableInfoBindData {
    catalog::Catalog* catalog;

    ShowConnectionBindData(catalog::Catalog* catalog, catalog::TableSchema* tableSchema,
        std::vector<std::unique_ptr<common::LogicalType>> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : TableInfoBindData{tableSchema, std::move(returnTypes), std::move(returnColumnNames),
              maxOffset},
          catalog{catalog} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<ShowConnectionBindData>(
            catalog, tableSchema, common::LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

struct ShowConnectionFunction final : public CallFunction {
    static function_set getFunctionSet();

    static void outputRelTableConnection(common::ValueVector* srcTableNameVector,
        common::ValueVector* dstTableNameVector, uint64_t outputPos, catalog::Catalog* catalog,
        common::table_id_t tableID);

    static void tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        TableFuncBindInput* input, catalog::Catalog* catalog,
        storage::StorageManager* /*storageManager*/);
};

struct StorageInfoSharedState final : public CallFuncSharedState {
    std::vector<storage::Column*> columns;

    StorageInfoSharedState(storage::Table* table, common::offset_t maxOffset);

private:
    void collectColumns(storage::Table* table);
    static std::vector<storage::Column*> collectColumns(storage::Column* column);
};

struct StorageInfoLocalState final : public TableFuncLocalState {
    std::unique_ptr<common::DataChunkCollection> dataChunkCollection;
    common::vector_idx_t currChunkIdx;

    explicit StorageInfoLocalState(storage::MemoryManager* mm) : currChunkIdx{0} {
        dataChunkCollection = std::make_unique<common::DataChunkCollection>(mm);
    }
};

struct StorageInfoBindData final : public CallTableFuncBindData {
    StorageInfoBindData(catalog::TableSchema* schema,
        std::vector<std::unique_ptr<common::LogicalType>> columnTypes,
        std::vector<std::string> columnNames, storage::Table* table)
        : CallTableFuncBindData{std::move(columnTypes), columnNames, 1 /*maxOffset*/},
          schema{schema}, table{table} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<StorageInfoBindData>(
            schema, common::LogicalType::copy(this->columnTypes), this->columnNames, table);
    }

    catalog::TableSchema* schema;
    storage::Table* table;
};

struct StorageInfoFunction final : public CallFunction {
    static function_set getFunctionSet();

    static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput& /*input*/,
        TableFuncSharedState* /*state*/, storage::MemoryManager* mm);
    static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input);

    static void tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
        TableFuncBindInput* input, catalog::Catalog* catalog,
        storage::StorageManager* storageManager);

private:
    static void appendStorageInfoForColumn(std::string tableType, common::DataChunk& outputChunk,
        StorageInfoLocalState* localState, const storage::Column* column);
    static void appendColumnChunkStorageInfo(common::node_group_idx_t nodeGroupIdx,
        const std::string& tableType, const storage::Column* column,
        common::DataChunk& outputChunk);
};

} // namespace function
} // namespace kuzu
