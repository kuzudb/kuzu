#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct CurrValFunction {
    static constexpr const char* name = "CURR_VAL";

    static function_set getFunctionSet();
};

struct NextValFunction {
    static constexpr const char* name = "NEXT_VAL";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
