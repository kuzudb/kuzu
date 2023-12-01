#pragma once

#include <cstdint>
#include <string>

#include "common/api.h"

namespace kuzu {
namespace common {

struct timestamp_t;
struct date_t;

enum class KUZU_API DatePartSpecifier : uint8_t {
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

struct KUZU_API interval_t {
    int32_t months = 0;
    int32_t days = 0;
    int64_t micros = 0;

    interval_t();
    interval_t(int32_t months_p, int32_t days_p, int64_t micros_p);

    // comparator operators
    bool operator==(const interval_t& rhs) const;
    bool operator!=(const interval_t& rhs) const;

    bool operator>(const interval_t& rhs) const;
    bool operator<=(const interval_t& rhs) const;
    bool operator<(const interval_t& rhs) const;
    bool operator>=(const interval_t& rhs) const;

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
    KUZU_API static constexpr const int32_t MONTHS_PER_YEAR = 12;
    KUZU_API static constexpr const int64_t MSECS_PER_SEC = 1000;
    KUZU_API static constexpr const int32_t SECS_PER_MINUTE = 60;
    KUZU_API static constexpr const int32_t MINS_PER_HOUR = 60;
    KUZU_API static constexpr const int32_t HOURS_PER_DAY = 24;
    // only used for interval comparison/ordering purposes, in which case a month counts as 30 days
    KUZU_API static constexpr const int64_t DAYS_PER_MONTH = 30;
    KUZU_API static constexpr const int64_t MONTHS_PER_QUARTER = 3;
    KUZU_API static constexpr const int64_t MONTHS_PER_MILLENIUM = 12000;
    KUZU_API static constexpr const int64_t MONTHS_PER_CENTURY = 1200;
    KUZU_API static constexpr const int64_t MONTHS_PER_DECADE = 120;

    KUZU_API static constexpr const int64_t MICROS_PER_MSEC = 1000;
    KUZU_API static constexpr const int64_t MICROS_PER_SEC = MICROS_PER_MSEC * MSECS_PER_SEC;
    KUZU_API static constexpr const int64_t MICROS_PER_MINUTE = MICROS_PER_SEC * SECS_PER_MINUTE;
    KUZU_API static constexpr const int64_t MICROS_PER_HOUR = MICROS_PER_MINUTE * MINS_PER_HOUR;
    KUZU_API static constexpr const int64_t MICROS_PER_DAY = MICROS_PER_HOUR * HOURS_PER_DAY;
    KUZU_API static constexpr const int64_t MICROS_PER_MONTH = MICROS_PER_DAY * DAYS_PER_MONTH;

    KUZU_API static constexpr const int64_t NANOS_PER_MICRO = 1000;

    KUZU_API static void addition(interval_t& result, uint64_t number, std::string specifierStr);
    KUZU_API static void parseIntervalField(
        std::string buf, uint64_t& pos, uint64_t len, interval_t& result);
    KUZU_API static interval_t fromCString(const char* str, uint64_t len);
    KUZU_API static std::string toString(interval_t interval);
    KUZU_API static bool greaterThan(const interval_t& left, const interval_t& right);
    KUZU_API static void normalizeIntervalEntries(
        interval_t input, int64_t& months, int64_t& days, int64_t& micros);
    KUZU_API static void tryGetDatePartSpecifier(std::string specifier, DatePartSpecifier& result);
    KUZU_API static int32_t getIntervalPart(DatePartSpecifier specifier, interval_t& timestamp);
    KUZU_API static int64_t getMicro(const interval_t& val);
    KUZU_API static int64_t getNanoseconds(const interval_t& val);
};

} // namespace common
} // namespace kuzu
