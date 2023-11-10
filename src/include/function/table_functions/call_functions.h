#pragma once

#include "catalog/catalog_content.h"
#include "common/vector/value_vector.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "main/client_context.h"

namespace kuzu {
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

    CallFuncSharedState(common::offset_t maxOffset) : maxOffset{maxOffset}, curOffset{0} {}

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
        : result{std::move(result)}, CallTableFuncBindData{std::move(returnTypes),
                                         std::move(returnColumnNames), maxOffset} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<CurrentSettingBindData>(
            result, common::LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

struct CurrentSettingFunction : public CallFunction {
    static function_set getFunctionSet();

    static void tableFunc(TableFunctionInput& data, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
        TableFuncBindInput* input, catalog::CatalogContent* /*catalog*/);
};

struct DBVersionFunction : public CallFunction {
    static function_set getFunctionSet();

    static void tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        TableFuncBindInput* /*input*/, catalog::CatalogContent* /*catalog*/);
};

struct ShowTablesBindData : public CallTableFuncBindData {
    std::vector<catalog::TableSchema*> tables;

    ShowTablesBindData(std::vector<catalog::TableSchema*> tables,
        std::vector<std::unique_ptr<common::LogicalType>> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : tables{std::move(tables)}, CallTableFuncBindData{std::move(returnTypes),
                                         std::move(returnColumnNames), maxOffset} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<ShowTablesBindData>(
            tables, common::LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

struct ShowTablesFunction : public CallFunction {
    static function_set getFunctionSet();

    static void tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        TableFuncBindInput* /*input*/, catalog::CatalogContent* catalog);
};

struct TableInfoBindData : public CallTableFuncBindData {
    catalog::TableSchema* tableSchema;

    TableInfoBindData(catalog::TableSchema* tableSchema,
        std::vector<std::unique_ptr<common::LogicalType>> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : tableSchema{tableSchema}, CallTableFuncBindData{std::move(returnTypes),
                                        std::move(returnColumnNames), maxOffset} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<TableInfoBindData>(
            tableSchema, common::LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

struct TableInfoFunction : public CallFunction {
    static function_set getFunctionSet();

    static void tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        TableFuncBindInput* input, catalog::CatalogContent* catalog);
};

struct ShowConnectionBindData : public TableInfoBindData {
    catalog::CatalogContent* catalog;

    ShowConnectionBindData(catalog::CatalogContent* catalog, catalog::TableSchema* tableSchema,
        std::vector<std::unique_ptr<common::LogicalType>> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : catalog{catalog}, TableInfoBindData{tableSchema, std::move(returnTypes),
                                std::move(returnColumnNames), maxOffset} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<ShowConnectionBindData>(
            catalog, tableSchema, common::LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

struct ShowConnectionFunction : public CallFunction {
    static function_set getFunctionSet();

    static void outputRelTableConnection(common::ValueVector* srcTableNameVector,
        common::ValueVector* dstTableNameVector, uint64_t outputPos,
        catalog::CatalogContent* catalog, common::table_id_t tableID);

    static void tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        TableFuncBindInput* input, catalog::CatalogContent* catalog);
};

} // namespace function
} // namespace kuzu
