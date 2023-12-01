#pragma once

#include "binder/expression/expression.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"

namespace kuzu {
namespace binder {

struct BoundFileScanInfo {
    function::TableFunction* copyFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    binder::expression_vector columns;
    std::shared_ptr<Expression> offset;

    BoundFileScanInfo(function::TableFunction* copyFunc,
        std::unique_ptr<function::TableFuncBindData> bindData, binder::expression_vector columns,
        std::shared_ptr<Expression> offset)
        : copyFunc{copyFunc}, bindData{std::move(bindData)}, columns{std::move(columns)},
          offset{std::move(offset)} {}
    BoundFileScanInfo(const BoundFileScanInfo& other)
        : copyFunc{other.copyFunc}, bindData{other.bindData->copy()}, columns{other.columns},
          offset{other.offset} {}

    inline std::unique_ptr<BoundFileScanInfo> copy() const {
        return std::make_unique<BoundFileScanInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
