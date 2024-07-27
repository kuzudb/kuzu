#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct CollectFunction {
    static constexpr const char* name = "COLLECT";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
