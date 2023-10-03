#pragma once

#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"

namespace kuzu {
namespace function {
struct ShowTablesFunction {
    inline static std::unique_ptr<TableFunctionDefinition> getDefinitions() {
        return std::make_unique<TableFunctionDefinition>("show_tables", tableFunc, bindFunc);
    }

    static void tableFunc(std::pair<common::offset_t, common::offset_t> morsel,
        function::TableFuncBindData* bindData, std::vector<common::ValueVector*> outputVectors);

    static std::unique_ptr<TableFuncBindData> bindFunc(
        main::ClientContext* context, TableFuncBindInput input, catalog::CatalogContent* catalog);
};
} // namespace function
} // namespace kuzu
