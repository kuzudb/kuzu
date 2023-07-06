#pragma once

#include "common/vector/value_vector.h"
#include "function/function_definition.h"
#include "main/client_context.h"

namespace kuzu {

namespace catalog {
class CatalogContent;
} // namespace catalog

namespace function {

struct TableFuncBindData;
struct TableFuncBindInput;

using table_func_t = std::function<void(std::pair<common::offset_t, common::offset_t>,
    function::TableFuncBindData*, std::vector<common::ValueVector*>)>;
using table_bind_func_t = std::function<std::unique_ptr<TableFuncBindData>(
    main::ClientContext* context, TableFuncBindInput input, catalog::CatalogContent* catalog)>;

struct TableFuncBindData {
    std::vector<common::LogicalType> returnTypes;
    std::vector<std::string> returnColumnNames;
    common::offset_t maxOffset;

    TableFuncBindData(std::vector<common::LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : returnTypes{std::move(returnTypes)},
          returnColumnNames{std::move(returnColumnNames)}, maxOffset{maxOffset} {}

    virtual ~TableFuncBindData() = default;

    virtual std::unique_ptr<TableFuncBindData> copy() {
        return std::make_unique<TableFuncBindData>(returnTypes, returnColumnNames, maxOffset);
    }
};

struct TableFuncBindInput {
    explicit TableFuncBindInput(std::vector<common::Value> inputs) : inputs{std::move(inputs)} {}
    std::vector<common::Value> inputs;
};

struct TableFunctionDefinition {
    std::string name;
    table_func_t tableFunc;
    table_bind_func_t bindFunc;

    TableFunctionDefinition(std::string name, table_func_t tableFunc, table_bind_func_t bindFunc)
        : name{std::move(name)}, tableFunc{std::move(tableFunc)}, bindFunc{std::move(bindFunc)} {}
};

struct TableInfoBindData : public TableFuncBindData {
    catalog::TableSchema* tableSchema;

    TableInfoBindData(catalog::TableSchema* tableSchema,
        std::vector<common::LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        common::offset_t maxOffset)
        : tableSchema{tableSchema}, TableFuncBindData{std::move(returnTypes),
                                        std::move(returnColumnNames), maxOffset} {}

    std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<TableInfoBindData>(
            tableSchema, returnTypes, returnColumnNames, maxOffset);
    }
};

struct TableInfoFunction {
    inline static std::unique_ptr<TableFunctionDefinition> getDefinitions() {
        return std::make_unique<TableFunctionDefinition>("table_info", tableFunc, bindFunc);
    }

    static void tableFunc(std::pair<common::offset_t, common::offset_t> morsel,
        function::TableFuncBindData* bindData, std::vector<common::ValueVector*> outputVectors);

    static std::unique_ptr<TableInfoBindData> bindFunc(
        main::ClientContext* context, TableFuncBindInput input, catalog::CatalogContent* catalog);
};

struct DBVersionFunction {
    inline static std::unique_ptr<TableFunctionDefinition> getDefinitions() {
        return std::make_unique<TableFunctionDefinition>("db_version", tableFunc, bindFunc);
    }

    static void tableFunc(std::pair<common::offset_t, common::offset_t> morsel,
        function::TableFuncBindData* bindData, std::vector<common::ValueVector*> outputVectors);

    static std::unique_ptr<TableFuncBindData> bindFunc(
        main::ClientContext* context, TableFuncBindInput input, catalog::CatalogContent* catalog);
};

struct CurrentSettingBindData : public TableFuncBindData {
    std::string result;

    CurrentSettingBindData(std::string result, std::vector<common::LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : result{std::move(result)}, TableFuncBindData{std::move(returnTypes),
                                         std::move(returnColumnNames), maxOffset} {}

    std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<CurrentSettingBindData>(
            result, returnTypes, returnColumnNames, maxOffset);
    }
};

struct CurrentSettingFunction {
    inline static std::unique_ptr<TableFunctionDefinition> getDefinitions() {
        return std::make_unique<TableFunctionDefinition>("current_setting", tableFunc, bindFunc);
    }

    static void tableFunc(std::pair<common::offset_t, common::offset_t> morsel,
        function::TableFuncBindData* bindData, std::vector<common::ValueVector*> outputVectors);

    static std::unique_ptr<TableFuncBindData> bindFunc(
        main::ClientContext* context, TableFuncBindInput input, catalog::CatalogContent* catalog);
};

} // namespace function
} // namespace kuzu
