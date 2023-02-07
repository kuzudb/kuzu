#pragma once

#include <string>

namespace kuzu {
namespace common {

// Type used to represent time (microseconds)
struct dtime_t {
    int64_t micros;

    dtime_t() = default;
    explicit inline dtime_t(int64_t micros_p) : micros(micros_p) {}
    inline dtime_t& operator=(int64_t micros_p) {
        micros = micros_p;
        return *this;
    }

    // explicit conversion
    explicit inline operator int64_t() const { return micros; }
    explicit inline operator double() const { return micros; }

    // comparison operators
    inline bool operator==(const dtime_t& rhs) const { return micros == rhs.micros; };
    inline bool operator!=(const dtime_t& rhs) const { return micros != rhs.micros; };
    inline bool operator<=(const dtime_t& rhs) const { return micros <= rhs.micros; };
    inline bool operator<(const dtime_t& rhs) const { return micros < rhs.micros; };
    inline bool operator>(const dtime_t& rhs) const { return micros > rhs.micros; };
    inline bool operator>=(const dtime_t& rhs) const { return micros >= rhs.micros; };
};

// Note: Aside from some minor changes, this implementation is copied from DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/time.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/time.cpp.
// For example, instead of using their idx_t type to refer to indices, we directly use uint64_t,
// which is the actual type of idx_t (so we say uint64_t len instead of idx_t len). When more
// functionality is needed, we should first consult these DuckDB links.
class Time {
public:
    // Convert a string in the format "hh:mm:ss" to a time object
    static dtime_t FromCString(const char* buf, uint64_t len);
    static bool TryConvertTime(const char* buf, uint64_t len, uint64_t& pos, dtime_t& result);

    // Convert a time object to a string in the format "hh:mm:ss"
    static std::string toString(dtime_t time);

    static dtime_t FromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds = 0);

    // Extract the time from a given timestamp object
    static void Convert(
        dtime_t time, int32_t& out_hour, int32_t& out_min, int32_t& out_sec, int32_t& out_micros);

    static bool IsValid(int32_t hour, int32_t minute, int32_t second, int32_t milliseconds);
};

} // namespace common
} // namespace kuzu
