#pragma once

#include "function/table/bind_data.h"

namespace kuzu {
namespace binder {

struct BoundTableFunction {
    function::TableFunction tableFunction;
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::shared_ptr<binder::Expression> offset;
};

} // namespace binder
} // namespace kuzu
