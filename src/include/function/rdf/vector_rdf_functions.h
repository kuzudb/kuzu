#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct RDFTypeFunction {
    static constexpr const char* name = "TYPE";

    static function_set getFunctionSet();
};

struct ValidatePredicateFunction {
    static constexpr const char* name = "VALIDATE_PREDICATE";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
