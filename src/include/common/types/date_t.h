#pragma once

#include "interval_t.h"

namespace kuzu {
namespace common {

struct timestamp_t;

// System representation of dates as the number of days since 1970-01-01.
struct date_t {
    int32_t days;

    date_t() = default;
    explicit inline date_t(int32_t days_p) : days(days_p) {}

    // Comparison operators with date_t.
    inline bool operator==(const date_t& rhs) const { return days == rhs.days; };
    inline bool operator!=(const date_t& rhs) const { return days != rhs.days; };
    inline bool operator<=(const date_t& rhs) const { return days <= rhs.days; };
    inline bool operator<(const date_t& rhs) const { return days < rhs.days; };
    inline bool operator>(const date_t& rhs) const { return days > rhs.days; };
    inline bool operator>=(const date_t& rhs) const { return days >= rhs.days; };

    // Comparison operators with timestamp_t.
    bool operator==(const timestamp_t& rhs) const;
    inline bool operator!=(const timestamp_t& rhs) const { return !(*this == rhs); };
    bool operator<(const timestamp_t& rhs) const;
    inline bool operator<=(const timestamp_t& rhs) const {
        return (*this) < rhs || (*this) == rhs;
    };
    inline bool operator>(const timestamp_t& rhs) const { return !(*this <= rhs); };
    inline bool operator>=(const timestamp_t& rhs) const { return !(*this < rhs); };

    // arithmetic operators
    inline date_t operator+(const int32_t& day) const { return date_t(this->days + day); };
    inline date_t operator-(const int32_t& day) const { return date_t(this->days - day); };

    date_t operator+(const interval_t& interval) const;
    date_t operator-(const interval_t& interval) const;

    int64_t operator-(const date_t& rhs) const;
};

inline date_t operator+(int64_t i, const date_t date) {
    return date + i;
}

// Note: Aside from some minor changes, this implementation is copied from DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/date.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/date.cpp.
// For example, instead of using their idx_t type to refer to indices, we directly use uint64_t,
// which is the actual type of idx_t (so we say uint64_t len instead of idx_t len). When more
// functionality is needed, we should first consult these DuckDB links.
class Date {
public:
    static const int32_t NORMAL_DAYS[13];
    static const int32_t CUMULATIVE_DAYS[13];
    static const int32_t LEAP_DAYS[13];
    static const int32_t CUMULATIVE_LEAP_DAYS[13];
    static const int32_t CUMULATIVE_YEAR_DAYS[401];
    static const int8_t MONTH_PER_DAY_OF_YEAR[365];
    static const int8_t LEAP_MONTH_PER_DAY_OF_YEAR[366];

    constexpr static const int32_t MIN_YEAR = -290307;
    constexpr static const int32_t MAX_YEAR = 294247;
    constexpr static const int32_t EPOCH_YEAR = 1970;

    constexpr static const int32_t YEAR_INTERVAL = 400;
    constexpr static const int32_t DAYS_PER_YEAR_INTERVAL = 146097;

    // Convert a string in the format "YYYY-MM-DD" to a date object
    static date_t FromCString(const char* str, uint64_t len);
    // Convert a date object to a string in the format "YYYY-MM-DD"
    static std::string toString(date_t date);
    // Try to convert text in a buffer to a date; returns true if parsing was successful
    static bool TryConvertDate(const char* buf, uint64_t len, uint64_t& pos, date_t& result);

    // private:
    // Returns true if (year) is a leap year, and false otherwise
    static bool IsLeapYear(int32_t year);
    // Returns true if the specified (year, month, day) combination is a valid
    // date
    static bool IsValid(int32_t year, int32_t month, int32_t day);
    // Extract the year, month and day from a given date object
    static void Convert(date_t date, int32_t& out_year, int32_t& out_month, int32_t& out_day);
    // Create a Date object from a specified (year, month, day) combination
    static date_t FromDate(int32_t year, int32_t month, int32_t day);

    // Helper function to parse two digits from a string (e.g. "30" -> 30, "03" -> 3, "3" -> 3)
    static bool ParseDoubleDigit(const char* buf, uint64_t len, uint64_t& pos, int32_t& result);

    static int32_t MonthDays(int32_t year, int32_t month);

    static std::string getDayName(date_t& date);

    static std::string getMonthName(date_t& date);

    static date_t getLastDay(date_t& date);

    static int32_t getDatePart(DatePartSpecifier specifier, date_t& date);

    static date_t trunc(DatePartSpecifier specifier, date_t& date);

    static inline int64_t getEpochNanoSeconds(const date_t& date) {
        return ((int64_t)date.days) * (Interval::MICROS_PER_DAY * Interval::NANOS_PER_MICRO);
    }

private:
    static void ExtractYearOffset(int32_t& n, int32_t& year, int32_t& year_offset);
};

} // namespace common
} // namespace kuzu
