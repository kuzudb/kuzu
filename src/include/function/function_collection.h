#pragma once

#include "function.h"

namespace kuzu {
namespace function {

using get_function_set_fun = std::function<function_set()>;

struct FunctionCollection {
    const char* name;

    get_function_set_fun getFunctionSetFunc;

    static FunctionCollection* getFunctions();
};

} // namespace function
} // namespace kuzu
