#pragma once

#include "src/common/include/type_utils.h"
#include "src/common/types/include/date_t.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct DayName {
    template<class T>
    static inline void operation(T& input, gf_string_t& result) {
        assert(false);
    }
};

template<>
inline void DayName::operation(date_t& input, gf_string_t& result) {
    string dayName = Date::getDayName(input);
    result.set(dayName);
}

template<>
inline void DayName::operation(timestamp_t& input, gf_string_t& result) {
    dtime_t time{};
    date_t date{};
    Timestamp::Convert(input, date, time);
    string dayName = Date::getDayName(date);
    result.set(dayName);
}

template<>
inline void DayName::operation(Value& input, gf_string_t& result) {
    switch (input.dataType.typeID) {
    case DATE: {
        DayName::operation(input.val.dateVal, result);
    } break;
    case TIMESTAMP: {
        DayName::operation(input.val.timestampVal, result);
    } break;
    default:
        throw RuntimeException(
            "Cannot call dayname on type: " + Types::dataTypeToString(input.dataType.typeID));
    }
}

struct MonthName {
    template<class T>
    static inline void operation(T& input, gf_string_t& result) {
        assert(false);
    }
};

template<>
inline void MonthName::operation(date_t& input, gf_string_t& result) {
    string monthName = Date::getMonthName(input);
    result.set(monthName);
}

template<>
inline void MonthName::operation(timestamp_t& input, gf_string_t& result) {
    dtime_t time{};
    date_t date{};
    Timestamp::Convert(input, date, time);
    string monthName = Date::getMonthName(date);
    result.set(monthName);
}

template<>
inline void MonthName::operation(Value& input, gf_string_t& result) {
    switch (input.dataType.typeID) {
    case DATE: {
        MonthName::operation(input.val.dateVal, result);
    } break;
    case TIMESTAMP: {
        MonthName::operation(input.val.timestampVal, result);
    } break;
    default:
        throw RuntimeException(
            "Cannot call monthname on type: " + Types::dataTypeToString(input.dataType.typeID));
    }
}

struct LastDay {
    template<class T>
    static inline void operation(T& input, date_t& result) {
        assert(false);
    }
};

template<>
inline void LastDay::operation(date_t& input, date_t& result) {
    result = Date::getLastDay(input);
}

template<>
inline void LastDay::operation(timestamp_t& input, date_t& result) {
    date_t date{};
    dtime_t time{};
    Timestamp::Convert(input, date, time);
    result = Date::getLastDay(date);
}

template<>
inline void LastDay::operation(Value& input, date_t& result) {
    switch (input.dataType.typeID) {
    case DATE: {
        LastDay::operation(input.val.dateVal, result);
    } break;
    case TIMESTAMP: {
        LastDay::operation(input.val.timestampVal, result);
    } break;
    default:
        throw RuntimeException(
            "Cannot call lastday on type: " + Types::dataTypeToString(input.dataType.typeID));
    }
}

struct DatePart {
    template<class LEFT_TYPE, class RIGHT_TYPE>
    static inline void operation(LEFT_TYPE& partSpecifier, RIGHT_TYPE& input, int64_t& result) {
        assert(false);
    }
};

template<>
inline void DatePart::operation(gf_string_t& partSpecifier, date_t& input, int64_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Date::getDatePart(specifier, input);
}

template<>
inline void DatePart::operation(gf_string_t& partSpecifier, timestamp_t& input, int64_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Timestamp::getTimestampPart(specifier, input);
}

template<>
inline void DatePart::operation(gf_string_t& partSpecifier, interval_t& input, int64_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Interval::getIntervalPart(specifier, input);
}

template<>
inline void DatePart::operation(Value& partSpecifier, Value& input, int64_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.val.strVal.getAsString(), specifier);
    switch (input.dataType.typeID) {
    case DATE: {
        DatePart::operation(partSpecifier.val.strVal, input.val.dateVal, result);
    } break;
    case TIMESTAMP: {
        DatePart::operation(partSpecifier.val.strVal, input.val.timestampVal, result);
    } break;
    case INTERVAL: {
        DatePart::operation(partSpecifier.val.strVal, input.val.intervalVal, result);
    } break;
    default:
        throw RuntimeException(
            "Cannot call date_part on type: " + Types::dataTypeToString(input.dataType.typeID));
    }
}

struct DateTrunc {
    template<class LEFT_TYPE, class RIGHT_TYPE>
    static inline void operation(LEFT_TYPE& partSpecifier, RIGHT_TYPE& input, RIGHT_TYPE& result) {
        assert(false);
    }
};

template<>
inline void DateTrunc::operation(gf_string_t& partSpecifier, date_t& input, date_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Date::trunc(specifier, input);
}

template<>
inline void DateTrunc::operation(
    gf_string_t& partSpecifier, timestamp_t& input, timestamp_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Timestamp::trunc(specifier, input);
}

template<>
inline void DateTrunc::operation(Value& partSpecifier, Value& input, Value& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.val.strVal.getAsString(), specifier);
    switch (input.dataType.typeID) {
    case DATE: {
        result.dataType.typeID = DATE;
        DateTrunc::operation(partSpecifier.val.strVal, input.val.dateVal, result.val.dateVal);
    } break;
    case TIMESTAMP: {
        result.dataType.typeID = TIMESTAMP;
        DateTrunc::operation(
            partSpecifier.val.strVal, input.val.timestampVal, result.val.timestampVal);
    } break;
    default:
        throw RuntimeException(
            "Cannot call date_trunc on type: " + Types::dataTypeToString(input.dataType.typeID));
    }
}

struct Greatest {
    template<class T>
    static inline void operation(T& left, T& right, T& result) {
        result = left > right ? left : right;
    }
};

template<>
inline void Greatest::operation(Value& left, Value& right, Value& result) {
    assert(left.dataType.typeID == right.dataType.typeID);
    result.dataType.typeID = left.dataType.typeID;
    switch (left.dataType.typeID) {
    case DATE: {
        Greatest::operation(left.val.dateVal, right.val.dateVal, result.val.dateVal);
    } break;
    case TIMESTAMP: {
        Greatest::operation(left.val.timestampVal, right.val.timestampVal, result.val.timestampVal);
    } break;
    default:
        throw RuntimeException(
            "Cannot call greatest on type: " + Types::dataTypeToString(left.dataType.typeID) +
            " with type: " + Types::dataTypeToString(right.dataType.typeID));
    }
}

struct Least {
    template<class T>
    static inline void operation(T& left, T& right, T& result) {
        result = left > right ? right : left;
    }
};

template<>
inline void Least::operation(Value& left, Value& right, Value& result) {
    assert(left.dataType.typeID == right.dataType.typeID);
    result.dataType.typeID = left.dataType.typeID;
    switch (left.dataType.typeID) {
    case DATE: {
        Least::operation(left.val.dateVal, right.val.dateVal, result.val.dateVal);
    } break;

    case TIMESTAMP: {
        Least::operation(left.val.timestampVal, right.val.timestampVal, result.val.timestampVal);
    } break;
    default:
        throw RuntimeException(
            "Cannot call least on type: " + Types::dataTypeToString(left.dataType.typeID) +
            " with type: " + Types::dataTypeToString(right.dataType.typeID));
    }
}

struct MakeDate {
    static inline void operation(int64_t& year, int64_t& month, int64_t& day, date_t& result) {
        result = Date::FromDate(year, month, day);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
