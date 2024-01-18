#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct GenRandomUUIDFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
