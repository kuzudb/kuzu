#pragma once

#include "src/common/include/type_utils.h"
#include "src/common/types/include/date_t.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct DayName {
    template<class T>
    static inline void operation(T& input, bool isNull, gf_string_t& result) {
        assert(false);
    }
};

struct MonthName {
    template<class T>
    static inline void operation(T& input, bool isNull, gf_string_t& result) {
        assert(false);
    }
};

struct LastDay {
    template<class T>
    static inline void operation(T& input, bool isNull, date_t& result) {
        assert(false);
    }
};

struct DatePart {
    template<class T>
    static inline void operation(
        gf_string_t& partSpecifier, T& input, int64_t& result, bool isLeftNull, bool isRightNull) {
        assert(false);
    }
};

struct DateTrunc {
    template<class T>
    static inline void operation(
        gf_string_t& partSpecifier, T& input, T& result, bool isLeftNull, bool isRightNull) {
        assert(false);
    }
};

struct Greatest {
    template<class T>
    static inline void operation(T& left, T& right, T& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left > right ? left : right;
    }
};

struct Least {
    template<class T>
    static inline void operation(T& left, T& right, T& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left > right ? right : left;
    }
};

template<>
inline void MonthName::operation(date_t& input, bool isNull, gf_string_t& result) {
    assert(!isNull);
    string monthName = Date::getMonthName(input);
    result.set(monthName);
}

template<>
inline void MonthName::operation(timestamp_t& input, bool isNull, gf_string_t& result) {
    assert(!isNull);
    dtime_t time;
    date_t date;
    Timestamp::Convert(input, date, time);
    string monthName = Date::getMonthName(date);
    result.set(monthName);
}

template<>
inline void MonthName::operation(Value& input, bool isNull, gf_string_t& result) {
    assert(!isNull);
    switch (input.dataType.typeID) {
    case DATE: {
        MonthName::operation(input.val.dateVal, isNull, result);
    } break;
    case TIMESTAMP: {
        MonthName::operation(input.val.timestampVal, isNull, result);
    } break;
    default:
        throw invalid_argument(
            "Cannot call monthname on type: " + Types::dataTypeToString(input.dataType.typeID));
    }
}

template<>
inline void DayName::operation(date_t& input, bool isNull, gf_string_t& result) {
    assert(!isNull);
    string dayName = Date::getDayName(input);
    result.set(dayName);
}

template<>
inline void DayName::operation(timestamp_t& input, bool isNull, gf_string_t& result) {
    assert(!isNull);
    dtime_t time;
    date_t date;
    Timestamp::Convert(input, date, time);
    string dayName = Date::getDayName(date);
    result.set(dayName);
}

template<>
inline void DayName::operation(Value& input, bool isNull, gf_string_t& result) {
    assert(!isNull);
    switch (input.dataType.typeID) {
    case DATE: {
        DayName::operation(input.val.dateVal, isNull, result);
    } break;
    case TIMESTAMP: {
        DayName::operation(input.val.timestampVal, isNull, result);
    } break;
    default:
        throw invalid_argument(
            "Cannot call dayname on type: " + Types::dataTypeToString(input.dataType.typeID));
    }
}

template<>
inline void LastDay::operation(date_t& input, bool isNull, date_t& result) {
    assert(!isNull);
    result = Date::getLastDay(input);
}

template<>
inline void LastDay::operation(timestamp_t& input, bool isNull, date_t& result) {
    assert(!isNull);
    date_t date;
    dtime_t time;
    Timestamp::Convert(input, date, time);
    result = Date::getLastDay(date);
}

template<>
inline void LastDay::operation(Value& input, bool isNull, date_t& result) {
    assert(!isNull);
    switch (input.dataType.typeID) {
    case DATE: {
        LastDay::operation(input.val.dateVal, isNull, result);
    } break;
    case TIMESTAMP: {
        LastDay::operation(input.val.timestampVal, isNull, result);
    } break;
    default:
        throw invalid_argument(
            "Cannot call lastday on type: " + Types::dataTypeToString(input.dataType.typeID));
    }
}

template<>
inline void DatePart::operation(
    gf_string_t& partSpecifier, date_t& input, int64_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Date::getDatePart(specifier, input);
}

template<>
inline void DatePart::operation(gf_string_t& partSpecifier, timestamp_t& input, int64_t& result,
    bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Timestamp::getTimestampPart(specifier, input);
}

template<>
inline void DatePart::operation(gf_string_t& partSpecifier, interval_t& input, int64_t& result,
    bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Interval::getIntervalPart(specifier, input);
}

template<>
inline void DatePart::operation(
    gf_string_t& partSpecifier, Value& input, int64_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    switch (input.dataType.typeID) {
    case DATE: {
        DatePart::operation(partSpecifier, input.val.dateVal, result, isLeftNull, isRightNull);
    } break;
    case TIMESTAMP: {
        DatePart::operation(partSpecifier, input.val.timestampVal, result, isLeftNull, isRightNull);
    } break;
    case INTERVAL: {
        DatePart::operation(partSpecifier, input.val.intervalVal, result, isLeftNull, isRightNull);
    } break;
    default:
        throw invalid_argument(
            "Cannot call date_part on type: " + Types::dataTypeToString(input.dataType.typeID));
    }
}

template<>
inline void DateTrunc::operation(
    gf_string_t& partSpecifier, date_t& input, date_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Date::trunc(specifier, input);
}

template<>
inline void DateTrunc::operation(gf_string_t& partSpecifier, timestamp_t& input,
    timestamp_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Timestamp::trunc(specifier, input);
}

template<>
inline void DateTrunc::operation(
    gf_string_t& partSpecifier, Value& input, Value& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    switch (input.dataType.typeID) {
    case DATE: {
        result.dataType.typeID = DATE;
        DateTrunc::operation(
            partSpecifier, input.val.dateVal, result.val.dateVal, isLeftNull, isRightNull);
    } break;
    case TIMESTAMP: {
        result.dataType.typeID = TIMESTAMP;
        DateTrunc::operation(partSpecifier, input.val.timestampVal, result.val.timestampVal,
            isLeftNull, isRightNull);
    } break;
    default:
        throw invalid_argument(
            "Cannot call date_trunc on type: " + Types::dataTypeToString(input.dataType.typeID));
    }
}

template<>
inline void Greatest::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    assert(left.dataType.typeID == right.dataType.typeID);
    result.dataType.typeID = left.dataType.typeID;
    switch (left.dataType.typeID) {
    case DATE: {
        Greatest::operation(
            left.val.dateVal, right.val.dateVal, result.val.dateVal, isLeftNull, isRightNull);
    } break;
    case TIMESTAMP: {
        Greatest::operation(left.val.timestampVal, right.val.timestampVal, result.val.timestampVal,
            isLeftNull, isRightNull);
    } break;
    default:
        throw invalid_argument(
            "Cannot call greatest on type: " + Types::dataTypeToString(left.dataType.typeID) +
            " with type: " + Types::dataTypeToString(right.dataType.typeID));
    }
}

template<>
inline void Least::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    assert(left.dataType.typeID == right.dataType.typeID);
    result.dataType.typeID = left.dataType.typeID;
    switch (left.dataType.typeID) {
    case DATE: {
        Least::operation(
            left.val.dateVal, right.val.dateVal, result.val.dateVal, isLeftNull, isRightNull);
    } break;

    case TIMESTAMP: {
        Least::operation(left.val.timestampVal, right.val.timestampVal, result.val.timestampVal,
            isLeftNull, isRightNull);
    } break;
    default:
        throw invalid_argument(
            "Cannot call least on type: " + Types::dataTypeToString(left.dataType.typeID) +
            " with type: " + Types::dataTypeToString(right.dataType.typeID));
    }
}

} // namespace operation
} // namespace function
} // namespace graphflow
