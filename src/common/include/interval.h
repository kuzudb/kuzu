#pragma once

#include "src/common/include/types.h"

// Note: Aside from some minor changes, this implementation is copied from DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/interval.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/interval.cpp.
// When more functionality is needed, we should first consult these DuckDB links.
namespace graphflow {
namespace common {
// The Interval class is a static class that holds helper functions for the Interval type.
class Interval {
public:
    static constexpr const int32_t MONTHS_PER_YEAR = 12;
    static constexpr const int64_t MSECS_PER_SEC = 1000;
    static constexpr const int32_t SECS_PER_MINUTE = 60;
    static constexpr const int32_t MINS_PER_HOUR = 60;
    static constexpr const int32_t HOURS_PER_DAY = 24;

    static constexpr const int64_t MICROS_PER_MSEC = 1000;
    static constexpr const int64_t MICROS_PER_SEC = MICROS_PER_MSEC * MSECS_PER_SEC;
    static constexpr const int64_t MICROS_PER_MINUTE = MICROS_PER_SEC * SECS_PER_MINUTE;
    static constexpr const int64_t MICROS_PER_HOUR = MICROS_PER_MINUTE * MINS_PER_HOUR;
    static constexpr const int64_t MICROS_PER_DAY = MICROS_PER_HOUR * HOURS_PER_DAY;

    static void addition(interval_t& result, uint64_t number, string specifierStr);
    static void parseIntervalField(
        const char* buf, uint64_t& pos, uint64_t len, interval_t& result);
    static interval_t FromCString(const char* str, uint64_t len);
    static string toString(interval_t interval);
};

} // namespace common
} // namespace graphflow
