#pragma once

#include <string>

namespace kuzu {
namespace common {

struct timestamp_t;
struct date_t;

enum class DatePartSpecifier : uint8_t {
    YEAR,
    MONTH,
    DAY,
    DECADE,
    CENTURY,
    MILLENNIUM,
    QUARTER,
    MICROSECOND,
    MILLISECOND,
    SECOND,
    MINUTE,
    HOUR,
};

struct interval_t {
    int32_t months = 0;
    int32_t days = 0;
    int64_t micros = 0;

    interval_t() = default;
    explicit inline interval_t(int32_t months_p, int32_t days_p, int64_t micros_p)
        : months(months_p), days(days_p), micros(micros_p) {}

    // comparator operators
    inline bool operator==(const interval_t& rhs) const {
        return this->days == rhs.days && this->months == rhs.months && this->micros == rhs.micros;
    };
    inline bool operator!=(const interval_t& rhs) const { return !(*this == rhs); }

    bool operator>(const interval_t& rhs) const;
    inline bool operator<=(const interval_t& rhs) const { return !(*this > rhs); }
    inline bool operator<(const interval_t& rhs) const { return !(*this >= rhs); }
    inline bool operator>=(const interval_t& rhs) const { return *this > rhs || *this == rhs; }

    // arithmetic operators
    interval_t operator+(const interval_t& rhs) const;
    timestamp_t operator+(const timestamp_t& rhs) const;
    date_t operator+(const date_t& rhs) const;
    interval_t operator-(const interval_t& rhs) const;

    interval_t operator/(const uint64_t& rhs) const;
};

// Note: Aside from some minor changes, this implementation is copied from DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/interval.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/interval.cpp.
// When more functionality is needed, we should first consult these DuckDB links.
// The Interval class is a static class that holds helper functions for the Interval type.
class Interval {
public:
    static constexpr const int32_t MONTHS_PER_YEAR = 12;
    static constexpr const int64_t MSECS_PER_SEC = 1000;
    static constexpr const int32_t SECS_PER_MINUTE = 60;
    static constexpr const int32_t MINS_PER_HOUR = 60;
    static constexpr const int32_t HOURS_PER_DAY = 24;
    static constexpr const int64_t DAYS_PER_MONTH =
        30; // only used for interval comparison/ordering purposes, in which case a month counts as
    // 30 days
    static constexpr const int64_t MONTHS_PER_QUARTER = 3;
    static constexpr const int64_t MONTHS_PER_MILLENIUM = 12000;
    static constexpr const int64_t MONTHS_PER_CENTURY = 1200;
    static constexpr const int64_t MONTHS_PER_DECADE = 120;

    static constexpr const int64_t MICROS_PER_MSEC = 1000;
    static constexpr const int64_t MICROS_PER_SEC = MICROS_PER_MSEC * MSECS_PER_SEC;
    static constexpr const int64_t MICROS_PER_MINUTE = MICROS_PER_SEC * SECS_PER_MINUTE;
    static constexpr const int64_t MICROS_PER_HOUR = MICROS_PER_MINUTE * MINS_PER_HOUR;
    static constexpr const int64_t MICROS_PER_DAY = MICROS_PER_HOUR * HOURS_PER_DAY;
    static constexpr const int64_t MICROS_PER_MONTH = MICROS_PER_DAY * DAYS_PER_MONTH;

    static constexpr const int64_t NANOS_PER_MICRO = 1000;

    static void addition(interval_t& result, uint64_t number, std::string specifierStr);
    static void parseIntervalField(
        std::string buf, uint64_t& pos, uint64_t len, interval_t& result);
    static interval_t FromCString(const char* str, uint64_t len);
    static std::string toString(interval_t interval);
    static bool GreaterThan(const interval_t& left, const interval_t& right);
    static void NormalizeIntervalEntries(
        interval_t input, int64_t& months, int64_t& days, int64_t& micros);
    static void TryGetDatePartSpecifier(std::string specifier_p, DatePartSpecifier& result);
    static int32_t getIntervalPart(DatePartSpecifier specifier, interval_t& timestamp);
    static int64_t getMicro(const interval_t& val) {
        return val.micros + val.months * MICROS_PER_MONTH + val.days * MICROS_PER_DAY;
    }
    static inline int64_t getNanoseconds(const interval_t& val) {
        return getMicro(val) * NANOS_PER_MICRO;
    }
};

} // namespace common
} // namespace kuzu
