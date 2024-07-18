#pragma once

#include "binder/expression/expression.h"
#include "function/table/bind_data.h"
#include "function/table_functions.h"

namespace kuzu {
namespace binder {

struct BoundTableScanSourceInfo {
    function::TableFunction func;
    std::unique_ptr<function::TableFuncBindData> bindData;
    binder::expression_vector columns;
    binder::expression_vector castedColumns;

    BoundTableScanSourceInfo(function::TableFunction func,
        std::unique_ptr<function::TableFuncBindData> bindData, expression_vector columns, expression_vector castedColumns)
        : func{func}, bindData{std::move(bindData)}, columns{std::move(columns)}, castedColumns{std::move(castedColumns)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundTableScanSourceInfo);

private:
    BoundTableScanSourceInfo(const BoundTableScanSourceInfo& other)
        : func{other.func}, bindData{other.bindData->copy()}, columns{other.columns}, castedColumns{other.castedColumns} {}
};

} // namespace binder
} // namespace kuzu
