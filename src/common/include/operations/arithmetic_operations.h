#pragma once

#include <cmath>
#include <cstdlib>

#include "src/common/include/date.h"
#include "src/common/include/interval.h"
#include "src/common/include/timestamp.h"
#include "src/common/include/value.h"

namespace graphflow {
namespace common {
namespace operation {

struct Add {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left + right;
    }
};

struct Subtract {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left - right;
    }
};

struct Multiply {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left * right;
    }
};

struct Divide {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left / right;
    }
};

struct Modulo {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        assert(false);
    }
};

struct Power {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = pow(left, right);
    }
};

struct Negate {
    template<class T>
    static inline void operation(T& input, bool isNull, T& result) {
        assert(!isNull);
        result = -input;
    }
};

struct Abs {
    template<class T>
    static inline void operation(T& input, bool isNull, T& result) {
        assert(!isNull);
        result = abs(input);
    }
};

struct Floor {
    template<class T>
    static inline void operation(T& input, bool isNull, T& result) {
        assert(!isNull);
        result = floor(input);
    }
};

struct Ceil {
    template<class T>
    static inline void operation(T& input, bool isNull, T& result) {
        assert(!isNull);
        result = ceil(input);
    }
};

/********************************************
 **                                        **
 **   Specialized Modulo implementations   **
 **                                        **
 ********************************************/

template<>
inline void Modulo::operation(
    int64_t& left, int64_t& right, int64_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = left % right;
}

template<>
inline void Modulo::operation(
    int64_t& left, double_t& right, double_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = fmod(left, right);
}

template<>
inline void Modulo::operation(
    double_t& left, int64_t& right, double_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = fmod(left, right);
}

template<>
inline void Modulo::operation(
    double_t& left, double_t& right, double_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = fmod(left, right);
}

/***************************************************
 **                                               **
 **   Specialized Concat and Add implementation   **
 **                                               **
 **************************************************/

template<>
inline void Add::operation(
    gf_string_t& left, gf_string_t& right, gf_string_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    auto len = left.len + right.len;
    if (len <= gf_string_t::SHORT_STR_LENGTH /* concat's result is short */) {
        memcpy(result.prefix, left.prefix, left.len);
        memcpy(result.prefix + left.len, right.prefix, right.len);
    } else {
        auto buffer = reinterpret_cast<char*>(result.overflowPtr);
        memcpy(buffer, left.getData(), left.len);
        memcpy(buffer + left.len, right.getData(), right.len);
        memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
    }
    result.len = len;
}

template<>
inline void Add::operation(
    date_t& left, interval_t& right, date_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    if (right.months != 0) {
        int32_t year, month, day, maxDayInMonth;
        Date::Convert(left, year, month, day);
        int32_t year_diff = right.months / Interval::MONTHS_PER_YEAR;
        year += year_diff;
        month += right.months - year_diff * Interval::MONTHS_PER_YEAR;
        if (month > Interval::MONTHS_PER_YEAR) {
            year++;
            month -= Interval::MONTHS_PER_YEAR;
        } else if (month <= 0) {
            year--;
            month += Interval::MONTHS_PER_YEAR;
        }
        // handle date overflow
        // example: 2020-01-31 + "1 months"
        maxDayInMonth = Date::MonthDays(year, month);
        day = day > maxDayInMonth ? maxDayInMonth : day;
        result = Date::FromDate(year, month, day);
    } else {
        result = left;
    }
    if (right.days != 0) {
        result.days += right.days;
    }
    if (right.micros != 0) {
        result.days += int32_t(right.micros / Interval::MICROS_PER_DAY);
    }
}

template<>
inline void Add::operation(
    timestamp_t& left, interval_t& right, timestamp_t& result, bool isLeftNull, bool isRightNull) {
    date_t date;
    date_t result_date;
    dtime_t time;
    Timestamp::Convert(left, date, time);
    Add::operation<date_t, interval_t, date_t>(date, right, result_date, isLeftNull, isRightNull);
    date = result_date;
    int64_t diff =
        right.micros - ((right.micros / Interval::MICROS_PER_DAY) * Interval::MICROS_PER_DAY);
    time.micros += diff;
    if (time.micros >= Interval::MICROS_PER_DAY) {
        time.micros -= Interval::MICROS_PER_DAY;
        date.days++;
    } else if (time.micros < 0) {
        time.micros += Interval::MICROS_PER_DAY;
        date.days--;
    }
    result = Timestamp::FromDatetime(date, time);
}

template<>
inline void Add::operation(
    interval_t& left, interval_t& right, interval_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result.months = left.months + right.months;
    result.days = left.days + right.days;
    result.micros = left.micros + right.micros;
}

/*********************************************
 **                                         **
 **   Specialized Subtract implementation   **
 **                                         **
 *********************************************/

template<>
inline void Subtract::operation(
    date_t& left, interval_t& right, date_t& result, bool isLeftNull, bool isRightNull) {
    interval_t inverseRight;
    inverseRight.months = -right.months;
    inverseRight.days = -right.days;
    inverseRight.micros = -right.micros;
    Add::operation<date_t, interval_t, date_t>(left, inverseRight, result, isLeftNull, isRightNull);
}

template<>
inline void Subtract::operation(
    date_t& left, date_t& right, int64_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = left.days - right.days;
}

template<>
inline void Subtract::operation(
    timestamp_t& left, timestamp_t& right, interval_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    uint64_t diff = abs(left.value - right.value);
    result.months = 0;
    result.days = diff / Interval::MICROS_PER_DAY;
    result.micros = diff % Interval::MICROS_PER_DAY;
    if (left.value < right.value) {
        result.days = -result.days;
        result.micros = -result.micros;
    }
}

template<>
inline void Subtract::operation(
    timestamp_t& left, interval_t& right, timestamp_t& result, bool isLeftNull, bool isRightNull) {
    interval_t inverseRight;
    inverseRight.months = -right.months;
    inverseRight.days = -right.days;
    inverseRight.micros = -right.micros;
    Add::operation<timestamp_t, interval_t, timestamp_t>(
        left, inverseRight, result, isLeftNull, isRightNull);
}

template<>
inline void Subtract::operation(
    interval_t& left, interval_t& right, interval_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result.months = left.months - right.months;
    result.days = left.days - right.days;
    result.micros = left.micros - right.micros;
}

template<>
inline void Divide::operation(
    interval_t& left, int64_t& right, interval_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    int32_t monthsRemainder = left.months % right;
    int32_t daysRemainder = (left.days + monthsRemainder * Interval::DAYS_PER_MONTH) % right;
    result.months = left.months / right;
    result.days = (left.days + monthsRemainder * Interval::DAYS_PER_MONTH) / right;
    result.micros = (left.micros + daysRemainder * Interval::MICROS_PER_DAY) / right;
}

/**********************************************
 **                                          **
 **   Specialized Value(s) implementations   **
 **                                          **
 **********************************************/

struct ArithmeticOnValues {
    template<class FUNC, const char* arithmeticOpStr>
    static void operation(
        Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
        switch (left.dataType) {
        case INT64:
            switch (right.dataType) {
            case INT64: {
                result.dataType = INT64;
                FUNC::operation(left.val.int64Val, right.val.int64Val, result.val.int64Val,
                    isLeftNull, isRightNull);
            } break;
            case DOUBLE: {
                result.dataType = DOUBLE;
                FUNC::operation(left.val.int64Val, right.val.doubleVal, result.val.doubleVal,
                    isLeftNull, isRightNull);
            } break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `INT64` and `" +
                                       TypeUtils::dataTypeToString(right.dataType) + "`");
            }
            break;
        case DOUBLE:
            switch (right.dataType) {
            case INT64: {
                result.dataType = DOUBLE;
                FUNC::operation(left.val.doubleVal, right.val.int64Val, result.val.doubleVal,
                    isLeftNull, isRightNull);
            } break;
            case DOUBLE: {
                result.dataType = DOUBLE;
                FUNC::operation(left.val.doubleVal, right.val.doubleVal, result.val.doubleVal,
                    isLeftNull, isRightNull);
            } break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `DOUBLE` and `" +
                                       TypeUtils::dataTypeToString(right.dataType) + "`");
            }
            break;
        default:
            throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `" +
                                   TypeUtils::dataTypeToString(left.dataType) + "` and `" +
                                   TypeUtils::dataTypeToString(right.dataType) + "`");
        }
    }

    template<class FUNC, const char* arithmeticOpStr>
    static void operation(Value& input, bool isNull, Value& result) {
        assert(!isNull);
        switch (input.dataType) {
        case INT64: {
            result.dataType = INT64;
            FUNC::operation(input.val.int64Val, isNull, result.val.int64Val);
        } break;
        case DOUBLE: {
            result.dataType = DOUBLE;
            FUNC::operation(input.val.doubleVal, isNull, result.val.doubleVal);
        } break;
        default:
            throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `" +
                                   TypeUtils::dataTypeToString(input.dataType) + "`");
        }
    }
};

static const char addStr[] = "add";
static const char subtractStr[] = "subtract";
static const char multiplyStr[] = "multiply";
static const char divideStr[] = "divide";
static const char moduloStr[] = "modulo";
static const char powerStr[] = "power";
static const char negateStr[] = "negate";
static const char absStr[] = "abs";
static const char floorStr[] = "floor";
static const char ceilStr[] = "ceil";

template<>
inline void Add::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    if (left.dataType == STRING) {
        assert(right.dataType == STRING);
        result.dataType = STRING;
        Add::operation(
            left.val.strVal, right.val.strVal, result.val.strVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == DATE && right.dataType == INTERVAL) {
        result.dataType = DATE;
        Add::operation(
            left.val.dateVal, right.val.intervalVal, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == DATE && right.dataType == INT64) {
        result.dataType = DATE;
        Add::operation(
            left.val.dateVal, right.val.int64Val, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == TIMESTAMP) {
        assert(right.dataType == INTERVAL);
        result.dataType = TIMESTAMP;
        Add::operation(left.val.timestampVal, right.val.intervalVal, result.val.timestampVal,
            isLeftNull, isRightNull);
        return;
    } else if (left.dataType == INTERVAL) {
        assert(right.dataType == INTERVAL);
        result.dataType = INTERVAL;
        Add::operation(left.val.intervalVal, right.val.intervalVal, result.val.intervalVal,
            isLeftNull, isRightNull);
        return;
    }
    ArithmeticOnValues::operation<Add, addStr>(left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Subtract::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    if (left.dataType == DATE && right.dataType == INTERVAL) {
        result.dataType = DATE;
        Subtract::operation(
            left.val.dateVal, right.val.intervalVal, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == DATE && right.dataType == INT64) {
        result.dataType = DATE;
        Subtract::operation(
            left.val.dateVal, right.val.int64Val, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == DATE && right.dataType == DATE) {
        result.dataType = INT64;
        Subtract::operation(
            left.val.dateVal, right.val.dateVal, result.val.int64Val, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == TIMESTAMP && right.dataType == INTERVAL) {
        result.dataType = TIMESTAMP;
        Subtract::operation(left.val.timestampVal, right.val.intervalVal, result.val.timestampVal,
            isLeftNull, isRightNull);
        return;
    } else if (left.dataType == TIMESTAMP && right.dataType == TIMESTAMP) {
        result.dataType = INTERVAL;
        Subtract::operation(left.val.timestampVal, right.val.timestampVal, result.val.intervalVal,
            isLeftNull, isRightNull);
        return;
    } else if (left.dataType == INTERVAL && right.dataType == INTERVAL) {
        result.dataType = INTERVAL;
        Subtract::operation(left.val.intervalVal, right.val.intervalVal, result.val.intervalVal,
            isLeftNull, isRightNull);
        return;
    }
    ArithmeticOnValues::operation<Subtract, subtractStr>(
        left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Multiply::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    ArithmeticOnValues::operation<Multiply, multiplyStr>(
        left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Divide::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    if (left.dataType == INTERVAL && right.dataType == INT64) {
        result.dataType = INTERVAL;
        Divide::operation(left.val.intervalVal, right.val.int64Val, result.val.intervalVal,
            isLeftNull, isRightNull);
        return;
    }
    ArithmeticOnValues::operation<Divide, divideStr>(left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Modulo::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    ArithmeticOnValues::operation<Modulo, moduloStr>(left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Power::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    ArithmeticOnValues::operation<Power, powerStr>(left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Negate::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Negate, negateStr>(operand, isNull, result);
}

template<>
inline void Abs::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Abs, absStr>(operand, isNull, result);
}

template<>
inline void Floor::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Floor, floorStr>(operand, isNull, result);
};

template<>
inline void Ceil::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Ceil, ceilStr>(operand, isNull, result);
};

} // namespace operation
} // namespace common
} // namespace graphflow
