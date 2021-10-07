#pragma once

#include "src/common/include/types.h"

namespace graphflow {
namespace common {

// Note: Aside from some minor changes, this implementation is copied from DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/timestamp.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/timestamp.cpp.
// For example, instead of using their idx_t type to refer to indices, we directly use uint64_t,
// which is the actual type of idx_t (so we say uint64_t len instead of idx_t len). When more
// functionality is needed, we should first consult these DuckDB links.
struct timestamp_struct {
    int32_t year;
    int8_t month;
    int8_t day;
    int8_t hour;
    int8_t min;
    int8_t sec;
    int16_t msec;
};

// The Timestamp class is a static class that holds helper functions for the Timestamp type.
// timestamp/datetime uses 64 bits, high 32 bits for date and low 32 bits for time
class Timestamp {
public:
    static timestamp_t FromCString(const char* str, uint64_t len);

    // Convert a timestamp object to a string in the format "YYYY-MM-DD hh:mm:ss"
    static string toString(timestamp_t timestamp);

    static date_t GetDate(timestamp_t timestamp);

    static dtime_t GetTime(timestamp_t timestamp);

    // Create a Timestamp object from a specified (date, time) combination
    static timestamp_t FromDatetime(date_t date, dtime_t time);

    // Extract the date and time from a given timestamp object
    static void Convert(timestamp_t timestamp, date_t& out_date, dtime_t& out_time);
};

} // namespace common
} // namespace graphflow
