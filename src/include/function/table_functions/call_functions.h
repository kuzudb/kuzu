#pragma once

#include "catalog/catalog_content.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
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

    CallTableFuncBindData(std::vector<common::LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : TableFuncBindData{std::move(returnTypes), std::move(returnColumnNames)}, maxOffset{
                                                                                       maxOffset} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CallTableFuncBindData>(columnTypes, columnNames, maxOffset);
    }
};

struct CallFunction {
    static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input);
};

struct CurrentSettingBindData : public CallTableFuncBindData {
    std::string result;

    CurrentSettingBindData(std::string result, std::vector<common::LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          result{std::move(result)} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CurrentSettingBindData>(
            result, columnTypes, columnNames, maxOffset);
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
    std::vector<catalog::TableCatalogEntry*> tables;

    ShowTablesBindData(std::vector<catalog::TableCatalogEntry*> tables,
        std::vector<common::LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        common::offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tables{std::move(tables)} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowTablesBindData>(tables, columnTypes, columnNames, maxOffset);
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
    catalog::TableCatalogEntry* tableEntry;

    TableInfoBindData(catalog::TableCatalogEntry* tableEntry,
        std::vector<common::LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        common::offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tableEntry{tableEntry} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<TableInfoBindData>(tableEntry, columnTypes, columnNames, maxOffset);
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

    ShowConnectionBindData(catalog::Catalog* catalog, catalog::TableCatalogEntry* tableEntry,
        std::vector<common::LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        common::offset_t maxOffset)
        : TableInfoBindData{tableEntry, std::move(returnTypes), std::move(returnColumnNames),
              maxOffset},
          catalog{catalog} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowConnectionBindData>(
            catalog, tableEntry, columnTypes, columnNames, maxOffset);
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
    StorageInfoBindData(catalog::TableCatalogEntry* tableEntry,
        std::vector<common::LogicalType> columnTypes, std::vector<std::string> columnNames,
        storage::Table* table)
        : CallTableFuncBindData{std::move(columnTypes), columnNames, 1 /*maxOffset*/},
          tableEntry{tableEntry}, table{table} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<StorageInfoBindData>(
            tableEntry, this->columnTypes, this->columnNames, table);
    }

    catalog::TableCatalogEntry* tableEntry;
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
