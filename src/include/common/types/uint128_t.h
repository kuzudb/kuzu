#pragma once

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>

#include "common/api.h"

#include "int128_t.h" // for conversion between signed and unsigned

namespace kuzu {
namespace common {

struct KUZU_API uint128_t;

struct int128_t; // forward-declaration

struct uint128_t {
    uint64_t low;
    uint64_t high;

    uint128_t() = default;
    uint128_t(int64_t value);  // NOLINT: Allow implicit conversion from numeric values
    uint128_t(int32_t value);  // NOLINT: Allow implicit conversion from numeric values
    uint128_t(int16_t value);  // NOLINT: Allow implicit conversion from numeric values
    uint128_t(int8_t value);   // NOLINT: Allow implicit conversion from numeric values
    uint128_t(uint64_t value); // NOLINT: Allow implicit conversion from numeric values
    uint128_t(uint32_t value); // NOLINT: Allow implicit conversion from numeric values
    uint128_t(uint16_t value); // NOLINT: Allow implicit conversion from numeric values
    uint128_t(uint8_t value);  // NOLINT: Allow implicit conversion from numeric values
    uint128_t(int128_t value); // NOLINT: Allow implicit conversion from numeric values
    uint128_t(double value);   // NOLINT: Allow implicit conversion from numeric values
    uint128_t(float value);    // NOLINT: Allow implicit conversion from numeric values

    constexpr uint128_t(uint64_t low, uint64_t high) : low(low), high(high) {}

    constexpr uint128_t(const uint128_t&) = default;
    constexpr uint128_t(uint128_t&&) = default;
    uint128_t& operator=(const uint128_t&) = default;
    uint128_t& operator=(uint128_t&&) = default;

    uint128_t operator-() const;

    // inplace arithmetic operators
    uint128_t& operator+=(const uint128_t& rhs);
    uint128_t& operator*=(const uint128_t& rhs);
    uint128_t& operator|=(const uint128_t& rhs);
    uint128_t& operator&=(const uint128_t& rhs);

    // cast operators
    explicit operator int64_t() const;
    explicit operator int32_t() const;
    explicit operator int16_t() const;
    explicit operator int8_t() const;
    explicit operator uint64_t() const;
    explicit operator uint32_t() const;
    explicit operator uint16_t() const;
    explicit operator uint8_t() const;
    explicit operator double() const;
    explicit operator float() const;
};

// arithmetic operators
KUZU_API uint128_t operator+(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API uint128_t operator-(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API uint128_t operator*(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API uint128_t operator/(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API uint128_t operator%(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API uint128_t operator^(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API uint128_t operator&(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API uint128_t operator~(const uint128_t& val);
KUZU_API uint128_t operator|(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API uint128_t operator<<(const uint128_t& lhs, int amount);
KUZU_API uint128_t operator>>(const uint128_t& lhs, int amount);

// comparison operators
KUZU_API bool operator==(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API bool operator!=(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API bool operator>(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API bool operator>=(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API bool operator<(const uint128_t& lhs, const uint128_t& rhs);
KUZU_API bool operator<=(const uint128_t& lhs, const uint128_t& rhs);

class Uint128_t {
public:
    static std::string ToString(uint128_t input);

    template<class T>
    static bool tryCast(uint128_t input, T& result);

    template<class T>
    static T Cast(uint128_t input) {
        T result;
        tryCast(input, result);
        return result;
    }

    template<class T>
    static bool tryCastTo(T value, uint128_t& result);

    template<class T>
    static uint128_t castTo(T value) {
        uint128_t result{};
        if (!tryCastTo(value, result)) {
            throw std::overflow_error("UINT128 is out of range");
        }
        return result;
    }

    static bool tryMultiply(uint128_t lhs, uint128_t rhs, uint128_t& result);

    static uint128_t Add(uint128_t lhs, uint128_t rhs);
    static uint128_t Sub(uint128_t lhs, uint128_t rhs);
    static uint128_t Mul(uint128_t lhs, uint128_t rhs);
    static uint128_t Div(uint128_t lhs, uint128_t rhs);
    static uint128_t Mod(uint128_t lhs, uint128_t rhs);
    static uint128_t Xor(uint128_t lhs, uint128_t rhs);
    static uint128_t LeftShift(uint128_t lhs, int amount);
    static uint128_t RightShift(uint128_t lhs, int amount);
    static uint128_t BinaryAnd(uint128_t lhs, uint128_t rhs);
    static uint128_t BinaryOr(uint128_t lhs, uint128_t rhs);
    static uint128_t BinaryNot(uint128_t val);

    static uint128_t divMod(uint128_t lhs, uint128_t rhs, uint128_t& remainder);
    static uint128_t divModPositive(uint128_t lhs, uint64_t rhs, uint64_t& remainder);

    static bool addInPlace(uint128_t& lhs, uint128_t rhs);
    static bool subInPlace(uint128_t& lhs, uint128_t rhs);

    // comparison operators
    static bool Equals(uint128_t lhs, uint128_t rhs) {
        return lhs.low == rhs.low && lhs.high == rhs.high;
    }

    static bool notEquals(uint128_t lhs, uint128_t rhs) {
        return lhs.low != rhs.low || lhs.high != rhs.high;
    }

    static bool greaterThan(uint128_t lhs, uint128_t rhs) {
        return (lhs.high > rhs.high) || (lhs.high == rhs.high && lhs.low > rhs.low);
    }

    static bool greaterThanOrEquals(uint128_t lhs, uint128_t rhs) {
        return (lhs.high > rhs.high) || (lhs.high == rhs.high && lhs.low >= rhs.low);
    }

    static bool lessThan(uint128_t lhs, uint128_t rhs) {
        return (lhs.high < rhs.high) || (lhs.high == rhs.high && lhs.low < rhs.low);
    }

    static bool lessThanOrEquals(uint128_t lhs, uint128_t rhs) {
        return (lhs.high < rhs.high) || (lhs.high == rhs.high && lhs.low <= rhs.low);
    }
    static const uint128_t powerOf10[40];
};

template<>
bool Uint128_t::tryCast(uint128_t input, int8_t& result);
template<>
bool Uint128_t::tryCast(uint128_t input, int16_t& result);
template<>
bool Uint128_t::tryCast(uint128_t input, int32_t& result);
template<>
bool Uint128_t::tryCast(uint128_t input, int64_t& result);
template<>
bool Uint128_t::tryCast(uint128_t input, uint8_t& result);
template<>
bool Uint128_t::tryCast(uint128_t input, uint16_t& result);
template<>
bool Uint128_t::tryCast(uint128_t input, uint32_t& result);
template<>
bool Uint128_t::tryCast(uint128_t input, uint64_t& result);
template<>
bool Uint128_t::tryCast(uint128_t input, int128_t& result); // unsigned to signed
template<>
bool Uint128_t::tryCast(uint128_t input, float& result);
template<>
bool Uint128_t::tryCast(uint128_t input, double& result);
template<>
bool Uint128_t::tryCast(uint128_t input, long double& result);

template<>
bool Uint128_t::tryCastTo(int8_t value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(int16_t value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(int32_t value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(int64_t value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(uint8_t value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(uint16_t value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(uint32_t value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(uint64_t value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(uint128_t value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(int128_t value, uint128_t& result); // signed to unsigned
template<>
bool Uint128_t::tryCastTo(float value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(double value, uint128_t& result);
template<>
bool Uint128_t::tryCastTo(long double value, uint128_t& result);

// TODO: const char to int128

} // namespace common
} // namespace kuzu

template<>
struct std::hash<kuzu::common::uint128_t> {
    std::size_t operator()(const kuzu::common::uint128_t& v) const noexcept;
};
