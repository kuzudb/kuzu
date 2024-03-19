#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct RDFTypeFunction {
    static function_set getFunctionSet();
};

struct ValidatePredicateFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
