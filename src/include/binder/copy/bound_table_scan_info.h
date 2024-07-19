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

    BoundTableScanSourceInfo(function::TableFunction func,
        std::unique_ptr<function::TableFuncBindData> bindData, binder::expression_vector columns)
        : func{func}, bindData{std::move(bindData)}, columns{std::move(columns)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundTableScanSourceInfo);

private:
    BoundTableScanSourceInfo(const BoundTableScanSourceInfo& other)
        : func{other.func}, bindData{other.bindData->copy()}, columns{other.columns} {}
};

} // namespace binder
} // namespace kuzu
