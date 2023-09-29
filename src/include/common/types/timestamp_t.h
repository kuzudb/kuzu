#pragma once

#include "date_t.h"
#include "dtime_t.h"

namespace kuzu {
namespace common {

// Type used to represent timestamps (value is in microseconds since 1970-01-01)
struct KUZU_API timestamp_t {
    int64_t value = 0;

    timestamp_t();
    explicit timestamp_t(int64_t value_p);
    timestamp_t& operator=(int64_t value_p);

    // explicit conversion
    explicit operator int64_t() const;

    // Comparison operators with timestamp_t.
    bool operator==(const timestamp_t& rhs) const;
    bool operator!=(const timestamp_t& rhs) const;
    bool operator<=(const timestamp_t& rhs) const;
    bool operator<(const timestamp_t& rhs) const;
    bool operator>(const timestamp_t& rhs) const;
    bool operator>=(const timestamp_t& rhs) const;

    // Comparison operators with date_t.
    bool operator==(const date_t& rhs) const;
    bool operator!=(const date_t& rhs) const;
    bool operator<(const date_t& rhs) const;
    bool operator<=(const date_t& rhs) const;
    bool operator>(const date_t& rhs) const;
    bool operator>=(const date_t& rhs) const;

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
    KUZU_API static timestamp_t fromCString(const char* str, uint64_t len);

    // Convert a timestamp object to a std::string in the format "YYYY-MM-DD hh:mm:ss".
    KUZU_API static std::string toString(timestamp_t timestamp);

    KUZU_API static date_t getDate(timestamp_t timestamp);

    KUZU_API static dtime_t getTime(timestamp_t timestamp);

    // Create a Timestamp object from a specified (date, time) combination.
    KUZU_API static timestamp_t fromDateTime(date_t date, dtime_t time);

    KUZU_API static bool tryConvertTimestamp(const char* str, uint64_t len, timestamp_t& result);

    // Extract the date and time from a given timestamp object.
    KUZU_API static void convert(timestamp_t timestamp, date_t& out_date, dtime_t& out_time);

    // Create a Timestamp object from the specified epochMs.
    KUZU_API static timestamp_t fromEpochMs(int64_t epochMs);

    // Create a Timestamp object from the specified epochSec.
    KUZU_API static timestamp_t fromEpochSec(int64_t epochSec);

    KUZU_API static int32_t getTimestampPart(DatePartSpecifier specifier, timestamp_t& timestamp);

    KUZU_API static timestamp_t trunc(DatePartSpecifier specifier, timestamp_t& date);

    KUZU_API static int64_t getEpochNanoSeconds(const timestamp_t& timestamp);

    KUZU_API static bool tryParseUTCOffset(
        const char* str, uint64_t& pos, uint64_t len, int& hour_offset, int& minute_offset);

private:
    static std::string getTimestampConversionExceptionMsg(const char* str, uint64_t len) {
        return "Error occurred during parsing timestamp. Given: \"" + std::string(str, len) +
               "\". Expected format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])";
    }
};

} // namespace common
} // namespace kuzu
