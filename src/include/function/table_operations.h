#pragma once

#include "common/vector/value_vector.h"
#include "function/function_definition.h"

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
    TableFuncBindInput input, catalog::CatalogContent* catalog)>;

struct TableFuncBindData {
    std::vector<common::LogicalType> returnTypes;
    std::vector<std::string> returnColumnNames;
    common::offset_t maxOffset;

    TableFuncBindData(std::vector<common::LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : returnTypes{std::move(returnTypes)},
          returnColumnNames{std::move(returnColumnNames)}, maxOffset{maxOffset} {}

    virtual ~TableFuncBindData() = default;

    virtual std::unique_ptr<TableFuncBindData> copy() = 0;
};

struct TableFuncBindInput {
    explicit TableFuncBindInput(std::vector<common::Value> inputs) : inputs{std::move(inputs)} {}
    std::vector<common::Value> inputs;
};

struct TableOperationDefinition {
    std::string name;
    table_func_t tableFunc;
    table_bind_func_t bindFunc;

    TableOperationDefinition(std::string name, table_func_t tableFunc, table_bind_func_t bindFunc)
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

struct TableInfoOperation {
    inline static std::unique_ptr<TableOperationDefinition> getDefinitions() {
        return std::make_unique<TableOperationDefinition>("table_info", tableFunc, bindFunc);
    }

    static void tableFunc(std::pair<common::offset_t, common::offset_t> morsel,
        function::TableFuncBindData* bindData, std::vector<common::ValueVector*> outputVectors);

    static std::unique_ptr<TableInfoBindData> bindFunc(
        TableFuncBindInput input, catalog::CatalogContent* catalog);
};

} // namespace function
} // namespace kuzu
