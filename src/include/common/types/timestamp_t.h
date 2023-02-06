#pragma once

#include "date_t.h"
#include "dtime_t.h"

namespace kuzu {
namespace common {

// Type used to represent timestamps (value is in microseconds since 1970-01-01)
struct timestamp_t {
    int64_t value = 0;

    timestamp_t() = default;
    explicit inline timestamp_t(int64_t value_p) : value(value_p) {}
    inline timestamp_t& operator=(int64_t value_p) {
        value = value_p;
        return *this;
    }

    // explicit conversion
    explicit inline operator int64_t() const { return value; }

    // Comparison operators with timestamp_t.
    inline bool operator==(const timestamp_t& rhs) const { return value == rhs.value; };
    inline bool operator!=(const timestamp_t& rhs) const { return value != rhs.value; };
    inline bool operator<=(const timestamp_t& rhs) const { return value <= rhs.value; };
    inline bool operator<(const timestamp_t& rhs) const { return value < rhs.value; };
    inline bool operator>(const timestamp_t& rhs) const { return value > rhs.value; };
    inline bool operator>=(const timestamp_t& rhs) const { return value >= rhs.value; };

    // Comparison operators with date_t.
    inline bool operator==(const date_t& rhs) const { return rhs == *this; };
    inline bool operator!=(const date_t& rhs) const { return !(rhs == *this); };
    inline bool operator<(const date_t& rhs) const { return rhs > *this; };
    inline bool operator<=(const date_t& rhs) const { return rhs >= *this; };
    inline bool operator>(const date_t& rhs) const { return rhs < *this; };
    inline bool operator>=(const date_t& rhs) const { return rhs <= *this; };

    // arithmetic operator
    timestamp_t operator+(const interval_t& interval) const;
    timestamp_t operator-(const interval_t& interval) const;

    interval_t operator-(const timestamp_t& rhs) const;
};

// Note: Aside from some minor changes, this implementation is copied from DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/timestamp.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/timestamp.cpp.
// For example, instead of using their idx_t type to refer to indices, we directly use uint64_t,
// which is the actual type of idx_t (so we say uint64_t len instead of idx_t len). When more
// functionality is needed, we should first consult these DuckDB links.

// The Timestamp class is a static class that holds helper functions for the Timestamp type.
// timestamp/datetime uses 64 bits, high 32 bits for date and low 32 bits for time
class Timestamp {
public:
    static timestamp_t FromCString(const char* str, uint64_t len);

    // Convert a timestamp object to a std::string in the format "YYYY-MM-DD hh:mm:ss".
    static std::string toString(timestamp_t timestamp);

    static date_t GetDate(timestamp_t timestamp);

    static dtime_t GetTime(timestamp_t timestamp);

    // Create a Timestamp object from a specified (date, time) combination.
    static timestamp_t FromDatetime(date_t date, dtime_t time);

    // Extract the date and time from a given timestamp object.
    static void Convert(timestamp_t timestamp, date_t& out_date, dtime_t& out_time);

    // Create a Timestamp object from the specified epochMs.
    static timestamp_t FromEpochMs(int64_t epochMs);

    // Create a Timestamp object from the specified epochSec.
    static timestamp_t FromEpochSec(int64_t epochSec);

    static int32_t getTimestampPart(DatePartSpecifier specifier, timestamp_t& timestamp);

    static timestamp_t trunc(DatePartSpecifier specifier, timestamp_t& date);

    static inline int64_t getEpochNanoSeconds(const timestamp_t& timestamp) {
        return timestamp.value * Interval::NANOS_PER_MICRO;
    }

    static bool TryParseUTCOffset(
        const char* str, uint64_t& pos, uint64_t len, int& hour_offset, int& minute_offset);

private:
    static std::string getTimestampConversionExceptionMsg(const char* str, uint64_t len) {
        return "Error occurred during parsing timestamp. Given: \"" + std::string(str, len) +
               "\". Expected format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])";
    }
};

} // namespace common
} // namespace kuzu
