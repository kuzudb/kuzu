#pragma once

#include "common/type_utils.h"
#include "common/types/date_t.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct DayName {
    template<class T>
    static inline void operation(T& input, ku_string_t& result) {
        assert(false);
    }
};

template<>
inline void DayName::operation(date_t& input, ku_string_t& result) {
    string dayName = Date::getDayName(input);
    result.set(dayName);
}

template<>
inline void DayName::operation(timestamp_t& input, ku_string_t& result) {
    dtime_t time{};
    date_t date{};
    Timestamp::Convert(input, date, time);
    string dayName = Date::getDayName(date);
    result.set(dayName);
}

struct MonthName {
    template<class T>
    static inline void operation(T& input, ku_string_t& result) {
        assert(false);
    }
};

template<>
inline void MonthName::operation(date_t& input, ku_string_t& result) {
    string monthName = Date::getMonthName(input);
    result.set(monthName);
}

template<>
inline void MonthName::operation(timestamp_t& input, ku_string_t& result) {
    dtime_t time{};
    date_t date{};
    Timestamp::Convert(input, date, time);
    string monthName = Date::getMonthName(date);
    result.set(monthName);
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

struct DatePart {
    template<class LEFT_TYPE, class RIGHT_TYPE>
    static inline void operation(LEFT_TYPE& partSpecifier, RIGHT_TYPE& input, int64_t& result) {
        assert(false);
    }
};

template<>
inline void DatePart::operation(ku_string_t& partSpecifier, date_t& input, int64_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Date::getDatePart(specifier, input);
}

template<>
inline void DatePart::operation(ku_string_t& partSpecifier, timestamp_t& input, int64_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Timestamp::getTimestampPart(specifier, input);
}

template<>
inline void DatePart::operation(ku_string_t& partSpecifier, interval_t& input, int64_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Interval::getIntervalPart(specifier, input);
}

struct DateTrunc {
    template<class LEFT_TYPE, class RIGHT_TYPE>
    static inline void operation(LEFT_TYPE& partSpecifier, RIGHT_TYPE& input, RIGHT_TYPE& result) {
        assert(false);
    }
};

template<>
inline void DateTrunc::operation(ku_string_t& partSpecifier, date_t& input, date_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Date::trunc(specifier, input);
}

template<>
inline void DateTrunc::operation(
    ku_string_t& partSpecifier, timestamp_t& input, timestamp_t& result) {
    DatePartSpecifier specifier;
    Interval::TryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = Timestamp::trunc(specifier, input);
}

struct Greatest {
    template<class T>
    static inline void operation(T& left, T& right, T& result) {
        result = left > right ? left : right;
    }
};

struct Least {
    template<class T>
    static inline void operation(T& left, T& right, T& result) {
        result = left > right ? right : left;
    }
};

struct MakeDate {
    static inline void operation(int64_t& year, int64_t& month, int64_t& day, date_t& result) {
        result = Date::FromDate(year, month, day);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
