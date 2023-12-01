#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct DatePartFunction {
    static function_set getFunctionSet();
};

struct DateTruncFunction {
    static function_set getFunctionSet();
};

struct DayNameFunction {
    static function_set getFunctionSet();
};

struct GreatestFunction {
    static function_set getFunctionSet();
};

struct LastDayFunction {
    static function_set getFunctionSet();
};

struct LeastFunction {
    static function_set getFunctionSet();
};

struct MakeDateFunction {
    static function_set getFunctionSet();
};

struct MonthNameFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
