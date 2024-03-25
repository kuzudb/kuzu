#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct DatePartFunction {
    static constexpr const char* name = "DATE_PART";

    static constexpr const char* alias = "DATEPART";

    static function_set getFunctionSet();
};

struct DateTruncFunction {
    static constexpr const char* name = "DATE_TRUNC";

    static constexpr const char* alias = "DATETRUNC";

    static function_set getFunctionSet();
};

struct DayNameFunction {
    static constexpr const char* name = "DAYNAME";

    static function_set getFunctionSet();
};

struct GreatestFunction {
    static constexpr const char* name = "GREATEST";

    static function_set getFunctionSet();
};

struct LastDayFunction {
    static constexpr const char* name = "LAST_DAY";

    static function_set getFunctionSet();
};

struct LeastFunction {
    static constexpr const char* name = "LEAST";

    static function_set getFunctionSet();
};

struct MakeDateFunction {
    static constexpr const char* name = "MAKE_DATE";

    static function_set getFunctionSet();
};

struct MonthNameFunction {
    static constexpr const char* name = "MONTHNAME";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
