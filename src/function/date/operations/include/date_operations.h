#pragma once

#include "src/common/include/type_utils.h"
#include "src/common/types/include/date_t.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct DayName {
    static inline void operation(date_t& input, bool isNull, gf_string_t& result) {
        assert(!isNull);
        string dayName = Date::getDayName(input);
        result.set(dayName);
    }
};

struct MonthName {
    static inline void operation(date_t& input, bool isNull, gf_string_t& result) {
        assert(!isNull);
        string monthName = Date::getMonthName(input);
        result.set(monthName);
    }
};

struct LastDay {
    static inline void operation(date_t& input, bool isNull, date_t& result) {
        assert(!isNull);
        result = Date::getLastDay(input);
    }
};

struct DatePart {
    static inline void operation(date_t& input, gf_string_t& partSpecifier, int64_t& result,
        bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = Date::getDatePart(input, partSpecifier.getAsString());
    }
};

struct DateTrunc {
    static inline void operation(date_t& input, gf_string_t partSpecifier, date_t& result,
        bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = Date::trunc(input, partSpecifier.getAsString());
    }
};

struct Greatest {
    static inline void operation(
        date_t& left, date_t& right, date_t& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left > right ? left : right;
    }
};

struct Least {
    static inline void operation(
        date_t& left, date_t& right, date_t& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left > right ? right : left;
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
