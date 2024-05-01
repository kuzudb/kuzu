#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct CoalesceFunction {
    static constexpr const char* name = "COALESCE";

    static function_set getFunctionSet();
};

struct IfNullFunction {
    static constexpr const char* name = "IFNULL";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
