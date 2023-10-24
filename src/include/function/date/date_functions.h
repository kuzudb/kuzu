#pragma once

#include "common/exception/not_implemented.h"
#include "common/type_utils.h"
#include "common/types/date_t.h"

namespace kuzu {
namespace function {

struct DayName {
    template<class T>
    static inline void operation(T& /*input*/, common::ku_string_t& /*result*/) {
        // LCOV_EXCL_START
        throw common::NotImplementedException("Invalid DayName() call on non-datetime object");
        // LCOV_EXCL_END
    }
};

template<>
inline void DayName::operation(common::date_t& input, common::ku_string_t& result) {
    std::string dayName = common::Date::getDayName(input);
    result.set(dayName);
}

template<>
inline void DayName::operation(common::timestamp_t& input, common::ku_string_t& result) {
    common::dtime_t time{};
    common::date_t date{};
    common::Timestamp::convert(input, date, time);
    std::string dayName = common::Date::getDayName(date);
    result.set(dayName);
}

struct MonthName {
    template<class T>
    static inline void operation(T& /*input*/, common::ku_string_t& /*result*/) {
        // LCOV_EXCL_START
        throw common::NotImplementedException("Invalid MonthName() call on non-datetime object");
        // LCOV_EXCL_END
    }
};

template<>
inline void MonthName::operation(common::date_t& input, common::ku_string_t& result) {
    std::string monthName = common::Date::getMonthName(input);
    result.set(monthName);
}

template<>
inline void MonthName::operation(common::timestamp_t& input, common::ku_string_t& result) {
    common::dtime_t time{};
    common::date_t date{};
    common::Timestamp::convert(input, date, time);
    std::string monthName = common::Date::getMonthName(date);
    result.set(monthName);
}

struct LastDay {
    template<class T>

    static inline void operation(T& /*input*/, common::date_t& /*result*/) {
        // LCOV_EXCL_START
        throw common::NotImplementedException("Invalid LastDay() call on non-datetime object");
        // LCOV_EXCL_END
    }
};

template<>
inline void LastDay::operation(common::date_t& input, common::date_t& result) {
    result = common::Date::getLastDay(input);
}

template<>
inline void LastDay::operation(common::timestamp_t& input, common::date_t& result) {
    common::date_t date{};
    common::dtime_t time{};
    common::Timestamp::convert(input, date, time);
    result = common::Date::getLastDay(date);
}

struct DatePart {
    template<class LEFT_TYPE, class RIGHT_TYPE>
    static inline void operation(
        LEFT_TYPE& /*partSpecifier*/, RIGHT_TYPE& /*input*/, int64_t& /*result*/) {
        // LCOV_EXCL_START
        throw common::NotImplementedException("Invalid DatePart() call on non-datetime object");
        // LCOV_EXCL_END
    }
};

template<>
inline void DatePart::operation(
    common::ku_string_t& partSpecifier, common::date_t& input, int64_t& result) {
    common::DatePartSpecifier specifier;
    common::Interval::tryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = common::Date::getDatePart(specifier, input);
}

template<>
inline void DatePart::operation(
    common::ku_string_t& partSpecifier, common::timestamp_t& input, int64_t& result) {
    common::DatePartSpecifier specifier;
    common::Interval::tryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = common::Timestamp::getTimestampPart(specifier, input);
}

template<>
inline void DatePart::operation(
    common::ku_string_t& partSpecifier, common::interval_t& input, int64_t& result) {
    common::DatePartSpecifier specifier;
    common::Interval::tryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = common::Interval::getIntervalPart(specifier, input);
}

struct DateTrunc {
    template<class LEFT_TYPE, class RIGHT_TYPE>
    static inline void operation(
        LEFT_TYPE& /*partSpecifier*/, RIGHT_TYPE& /*input*/, RIGHT_TYPE& /*result*/) {
        // LCOV_EXCL_START
        throw common::NotImplementedException("Invalid DatePart() call on non-datetime object");
        // LCOV_EXCL_END
    }
};

template<>
inline void DateTrunc::operation(
    common::ku_string_t& partSpecifier, common::date_t& input, common::date_t& result) {
    common::DatePartSpecifier specifier;
    common::Interval::tryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = common::Date::trunc(specifier, input);
}

template<>
inline void DateTrunc::operation(
    common::ku_string_t& partSpecifier, common::timestamp_t& input, common::timestamp_t& result) {
    common::DatePartSpecifier specifier;
    common::Interval::tryGetDatePartSpecifier(partSpecifier.getAsString(), specifier);
    result = common::Timestamp::trunc(specifier, input);
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
    static inline void operation(
        int64_t& year, int64_t& month, int64_t& day, common::date_t& result) {
        result = common::Date::fromDate(year, month, day);
    }
};

} // namespace function
} // namespace kuzu
