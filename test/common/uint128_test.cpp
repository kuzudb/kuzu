#include <cstdint>

#include "common/exception/overflow.h"

#include "common/types/uint128_t.h"
#include "common/types/int128_t.h"
#include "gtest/gtest.h"


using namespace kuzu::common;

TEST(Uint128Tests, ImplicitConversionToUint128) {
    { // conversion from int32_t
        uint128_t value = 42;
        EXPECT_EQ(value.low, 42);
        EXPECT_EQ(value.high, 0);
    }
    
    { // conversion from int128_t 2^64 + 1
        uint128_t value = int128_t((uint64_t)1, (int64_t)1);
        EXPECT_EQ(value.low, 1);
        EXPECT_EQ(value.high, 1);
    }

    { // conversion to int128_t
        uint128_t value = {35, 165};
        int128_t int128_value = value;
        EXPECT_EQ(int128_value.low, 35);
        EXPECT_EQ(int128_value.high, 165);
    }

    { // conversion from int128_t (error: negative value)
        int128_t negative_two = -2;
        EXPECT_THROW(uint128_t value = negative_two, OverflowException);
    }

    { // conversion to int128_t (error: overflow)
        uint128_t max_uint128 = {UINT64_MAX, UINT64_MAX};
        EXPECT_THROW(int128_t value = max_uint128, OverflowException);
    }
}

TEST(Uint128Tests, AdditionTest) {
    { // adding in upper bits: (1 << 64) + (2 << 64) = (3 << 64)
        uint128_t value1{0, 1};
        uint128_t value2{0, 2};
        uint128_t expected_value{0, 3};
        EXPECT_EQ(expected_value, value1 + value2);
        EXPECT_EQ(expected_value, Uint128_t::Add(value1, value2));
    }

    { // adding with overflow from lower to upper bits: (2^64 - 1) + 1 = (1 << 64)
        uint128_t value1{UINT64_MAX, 0};
        uint128_t value2{1, 0};
        uint128_t expected_value{0, 1};
        EXPECT_EQ(expected_value, value1 + value2);
        EXPECT_EQ(expected_value, Uint128_t::Add(value1, value2));
    }

    { // (2^127 - 1) + 1 = 2^128
        uint128_t value1{UINT64_MAX, INT64_MAX}; // 01111111...111
        uint128_t value2{1, 0};
        uint128_t expected_value{0, (1ULL << 63)};
        EXPECT_EQ(expected_value, value1 + value2);
        EXPECT_EQ(expected_value, Uint128_t::Add(value1, value2));
    }

    { // overflow
        uint128_t value1{UINT64_MAX, UINT64_MAX}; // 11111111...111
        uint128_t value2{1, 0};
        EXPECT_THROW(Uint128_t::Add(value1, value2), OverflowException);
    }
}

TEST(Uint128Tests, SubtractionTest) {
    { // subtracting in upper bits: (3 << 64) - (2 << 64) = (1 << 64)
        uint128_t value1{0, 3};
        uint128_t value2{0, 2};
        uint128_t expected_value{0, 1};
        EXPECT_EQ(expected_value, value1 - value2);
        EXPECT_EQ(expected_value, Uint128_t::Sub(value1, value2));
    }

    { // subtracting with underflow from upper to lower bits: (1 << 64) - 1 = (2^64 - 1)
        uint128_t value1{0, 1};
        uint128_t value2{1, 0};
        uint128_t expected_value{UINT64_MAX, 0};
        EXPECT_EQ(expected_value, value1 - value2);
        EXPECT_EQ(expected_value, Uint128_t::Sub(value1, value2));
    }

    { // (2^127) - 1 = (2^127 - 1)
        uint128_t value1{0, (1ULL << 63)}; // 10000000...000
        uint128_t value2{1, 0};
        uint128_t expected_value{UINT64_MAX, INT64_MAX}; // 01111111...111
        EXPECT_EQ(expected_value, value1 - value2);
        EXPECT_EQ(expected_value, Uint128_t::Sub(value1, value2));
    }

    { // underflow
        uint128_t value1{1, 0};
        uint128_t value2{2, 0};
        EXPECT_THROW(Uint128_t::Sub(value1, value2), OverflowException);
    }

    {
        uint128_t value1{0, 3};
        uint128_t value2{5, 3};
        EXPECT_THROW(Uint128_t::Sub(value1, value2), OverflowException);
    }
}


TEST(Uint128Tests, MultiplicationTest) {
    { // multiplying in lower bits: 3 * 5 = 15
        uint128_t value1{3, 0};
        uint128_t value2{5, 0};
        uint128_t expected_value{15, 0};
        EXPECT_EQ(expected_value, value1 * value2);
        EXPECT_EQ(expected_value, Uint128_t::Mul(value1, value2));
    }

    { // multiplying in upper bits: (1 << 64) * 3 = (3 << 64)
        uint128_t value1{0, 1};
        uint128_t value2{3, 0};
        uint128_t expected_value{0, 3};
        EXPECT_EQ(expected_value, value1 * value2);
        EXPECT_EQ(expected_value, Uint128_t::Mul(value1, value2));
    }

    { // multiplying with overflow from lower to upper bits: (2^64 - 1) * 2 = (2^65 - 2)
        uint128_t value1{UINT64_MAX, 0};
        uint128_t value2{2, 0};
        uint128_t expected_value{UINT64_MAX - 1, 1};
        EXPECT_EQ(expected_value, value1 * value2);
        EXPECT_EQ(expected_value, Uint128_t::Mul(value1, value2));
    }

    { // (2^64) * (2^63) = (2^127)
        uint128_t value1{0, 1};
        uint128_t value2{(1ULL << 63), 0};
        uint128_t expected_value{0, (1ULL << 63)};
        EXPECT_EQ(expected_value, value1 * value2);
        EXPECT_EQ(expected_value, Uint128_t::Mul(value1, value2));
    }

    { // overflow
        uint128_t value1{UINT64_MAX, UINT64_MAX}; // 11111111...111
        uint128_t value2{2, 0};
        EXPECT_THROW(Uint128_t::Mul(value1, value2), OverflowException);
    }
}
