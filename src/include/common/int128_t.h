#pragma once

#include <stdint.h>

#include "common/api.h"
#include "common/string_utils.h"

namespace kuzu::common {

struct int128_t;

// System representation for int128_t.
struct KUZU_API int128_t {
    uint64_t low;
    int64_t high;

    int128_t() = default;
    int128_t(int64_t value); // NOLINT: Allow implicit conversion from `int64_t`
    constexpr int128_t(uint64_t low, int64_t high) : low(low), high(high) {}

    constexpr int128_t(const int128_t&) = default;
    constexpr int128_t(int128_t&&) = default;
    int128_t& operator=(const int128_t&) = default;
    int128_t& operator=(int128_t&&) = default;

    // comparison operators
    bool operator==(const int128_t& rhs) const;
    bool operator!=(const int128_t& rhs) const;
    bool operator>(const int128_t& rhs) const;
    bool operator>=(const int128_t& rhs) const;
    bool operator<(const int128_t& rhs) const;
    bool operator<=(const int128_t& rhs) const;

    // arithmetic operators
    int128_t operator+(const int128_t& rhs) const;
    int128_t operator-(const int128_t& rhs) const;
    int128_t operator*(const int128_t& rhs) const;
    int128_t operator/(const int128_t& rhs) const;
    int128_t operator%(const int128_t& rhs) const;
    int128_t operator-() const;

    // inplace arithmetic operators
    int128_t& operator+=(const int128_t& rhs);
    int128_t& operator*=(const int128_t& rhs);

    // cast operators
    explicit operator int64_t() const;
};

// Note: this implementation is based on DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/main/src/include/duckdb/common/types/hugeint.hpp
// https://github.com/duckdb/duckdb/blob/main/src/include/duckdb/common/hugeint.hpp
// https://github.com/duckdb/duckdb/blob/main/src/common/types/hugeint.cpp

} // namespace kuzu::common
