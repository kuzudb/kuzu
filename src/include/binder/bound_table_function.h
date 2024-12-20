#pragma once

#include "function/table/bind_data.h"

namespace kuzu {
namespace binder {

struct BoundTableFunction {
    std::unique_ptr<function::TableFunction> tableFunction;
    std::unique_ptr<function::TableFuncBindData> bindData;
};

} // namespace binder
} // namespace kuzu
