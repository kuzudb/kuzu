#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct CreateGraphFunction {
    static constexpr const char* name = "GRAPH";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
