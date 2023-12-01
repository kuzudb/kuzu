#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct CenturyFunction {
    static function_set getFunctionSet();
};

struct EpochMsFunction {
    static function_set getFunctionSet();
};

struct ToTimestampFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
