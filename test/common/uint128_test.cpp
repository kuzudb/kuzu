#include <cstdint>

#include "common/exception/overflow.h"
#include "common/exception/runtime.h"

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
        EXPECT_THROW({
            [[maybe_unused]] uint128_t value = negative_two;
        }, OverflowException);
    }

    { // conversion to int128_t (error: overflow)
        uint128_t max_uint128 = {UINT64_MAX, UINT64_MAX};
        EXPECT_THROW({
            [[maybe_unused]] int128_t value = max_uint128;
        }, OverflowException);
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
        EXPECT_THROW(value1 + value2, OverflowException);
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
        EXPECT_THROW(value1 - value2, OverflowException);
    }

    {
        uint128_t value1{0, 3};
        uint128_t value2{5, 3};
        EXPECT_THROW(value1 - value2, OverflowException);
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
        EXPECT_THROW(value1 * value2, OverflowException);
    }

    { // overflow 2
        uint128_t value1{5, 0};
        uint128_t value2{0, (314ULL << 55)};
        EXPECT_THROW(value1 * value2, OverflowException);
    }
}

TEST(Uint128Tests, DivisionTest) {
    {
        uint128_t value1{1137, 0};
        uint128_t value2{3, 0};
        uint128_t expected_value{379, 0};
        EXPECT_EQ(expected_value, value1 / value2);
        EXPECT_EQ(expected_value, Uint128_t::Div(value1, value2));
    }

    { // integer division (truncating)
        uint128_t value1{1138, 0};
        uint128_t value2{3, 0};
        uint128_t expected_value{379, 0};
        EXPECT_EQ(expected_value, value1 / value2);
        EXPECT_EQ(expected_value, Uint128_t::Div(value1, value2));
    }

    { // division in upper bits
        uint128_t value1{0, (1ULL << 63)}; // 100...000
        uint128_t value2{0, (1ULL << 62)}; // 010...000
        uint128_t expected_value{2, 0};
        EXPECT_EQ(expected_value, value1 / value2);
        EXPECT_EQ(expected_value, Uint128_t::Div(value1, value2));
    }

    { // division in upper bits with truncation
        uint128_t value1{3528930, (1ULL << 63) + (1ULL << 61)}; // 101...XXX
        uint128_t value2{0, (1ULL << 62)}; // 010...000
        uint128_t expected_value{2, 0};
        EXPECT_EQ(expected_value, value1 / value2);
        EXPECT_EQ(expected_value, Uint128_t::Div(value1, value2));
    }

    {
        uint128_t value1{UINT64_MAX, UINT64_MAX};
        uint128_t value2{UINT64_MAX, UINT64_MAX};
        uint128_t expected_value{1, 0};
        EXPECT_EQ(expected_value, value1 / value2);
        EXPECT_EQ(expected_value, Uint128_t::Div(value1, value2));
    }

    {
        uint128_t value1{UINT64_MAX, INT64_MAX}; // 011...111
        uint128_t value2{0, (1ULL << 63)}; // 100...000
        uint128_t expected_value{0, 0};
        EXPECT_EQ(expected_value, value1 / value2);
        EXPECT_EQ(expected_value, Uint128_t::Div(value1, value2));
    }

    { // division by 0
        uint128_t value1{4, 5};
        uint128_t value2{0, 0};
        EXPECT_THROW(value1 / value2, RuntimeException);
    }
}

TEST(Uint128Tests, ModuloTest) {
    {
        uint128_t value1{1139, 0};
        uint128_t value2{3, 0};
        uint128_t expected_value{2, 0};
        EXPECT_EQ(expected_value, value1 % value2);
        EXPECT_EQ(expected_value, Uint128_t::Mod(value1, value2));
    }

    {
        uint128_t value1{1137, 0};
        uint128_t value2{3, 0};
        uint128_t expected_value{0, 0};
        EXPECT_EQ(expected_value, value1 % value2);
        EXPECT_EQ(expected_value, Uint128_t::Mod(value1, value2));
    }

    {
        uint128_t value1{UINT64_MAX, INT64_MAX}; // 011...111
        uint128_t value2{0, (1ULL << 63)}; // 100...000
        uint128_t expected_value{UINT64_MAX, INT64_MAX};
        EXPECT_EQ(expected_value, value1 % value2);
        EXPECT_EQ(expected_value, Uint128_t::Mod(value1, value2));
    }

    { // modulo by 0
        uint128_t value1{INT32_MAX, 73};
        uint128_t value2{0, 0};
        EXPECT_THROW(value1 % value2, RuntimeException);
    }
}

TEST(Uint128Tests, BitwiseTest1) {
    { // small numbers
        uint128_t value1{0b10100000, 0b01111001};
        uint128_t value2{0b11000110, 0b10101011};
        uint128_t expected_and{0b10000000, 0b00101001};
        uint128_t expected_or{0b11100110, 0b11111011};
        uint128_t expected_xor{0b01100110, 0b11010010};
        uint128_t expected_not1{0xffffffffffffff00 + 0b01011111, 0xffffffffffffff00 + 0b10000110};
        uint128_t expected_not2{0xffffffffffffff00 + 0b00111001, 0xffffffffffffff00 + 0b01010100};
        uint128_t expected_left_shift1{(uint64_t)(0b10100000) << 55, (uint64_t)(0b01111001) << 55};
        uint128_t expected_left_shift2{0, (1ULL << 63)};
        EXPECT_EQ(expected_and, value1 & value2);
        EXPECT_EQ(expected_and, Uint128_t::BinaryAnd(value1, value2));
        EXPECT_EQ(expected_or, value1 | value2);
        EXPECT_EQ(expected_or, Uint128_t::BinaryOr(value1, value2));
        EXPECT_EQ(expected_xor, value1 ^ value2);
        EXPECT_EQ(expected_xor, Uint128_t::Xor(value1, value2));
        EXPECT_EQ(expected_not1, ~value1);
        EXPECT_EQ(expected_not1, Uint128_t::BinaryNot(value1));
        EXPECT_EQ(expected_not2, ~value2);
        EXPECT_EQ(expected_not2, Uint128_t::BinaryNot(value2));
        EXPECT_EQ(value1, value1 << 0); // left shift by 0
        EXPECT_EQ(expected_left_shift1, value1 << 55);
        EXPECT_EQ(expected_left_shift1, Uint128_t::LeftShift(value1, 55));
        EXPECT_EQ(expected_left_shift2, value2 << 126);
        EXPECT_EQ(expected_left_shift2, Uint128_t::LeftShift(value2, 126));
    }
}

TEST(Uint128Tests, BitwiseTest2) {
    { // large numbers (with most significant bit set)
        uint128_t value1{0, (1ULL << 63) + (1ULL << 62)}; // 11000...000
        uint128_t value2{2, (1ULL << 62) + (1ULL << 60)}; // 01010...010
        uint128_t expected_and{0, (1ULL << 62)}; // 01000...000
        uint128_t expected_or{2, (1ULL << 63) + (1ULL << 62) + (1ULL << 60)}; // 11010...010
        uint128_t expected_xor{2, (1ULL << 63) + (1ULL << 60)}; // 10010...010
        uint128_t expected_not1{UINT64_MAX, (1ULL << 62) - 1}; // 00111...111
        uint128_t expected_right_shift1{3, 0}; // 000...011
        uint128_t expected_right_shift2{(1ULL << 62), 1}; // 000...1 010... 0 (shift by 62)
        EXPECT_EQ(expected_and, value1 & value2);
        EXPECT_EQ(expected_and, Uint128_t::BinaryAnd(value1, value2));
        EXPECT_EQ(expected_or, value1 | value2);
        EXPECT_EQ(expected_or, Uint128_t::BinaryOr(value1, value2));
        EXPECT_EQ(expected_xor, value1 ^ value2);
        EXPECT_EQ(expected_xor, Uint128_t::Xor(value1, value2));
        EXPECT_EQ(expected_not1, ~value1);
        EXPECT_EQ(expected_not1, Uint128_t::BinaryNot(value1));
        EXPECT_EQ(value1, value1 >> 0); // right shift by 0
        EXPECT_EQ(expected_right_shift1, value1 >> 126);
        EXPECT_EQ(expected_right_shift1, Uint128_t::RightShift(value1, 126));
        EXPECT_EQ(expected_right_shift2, value2 >> 62);
        EXPECT_EQ(expected_right_shift2, Uint128_t::RightShift(value2, 62));
    }
}

// Bit shifting by amounts that are > 127 or negative are currently undefined behavior.
// TEST(Uint128Tests, ExcessiveBitshift) {
//     {
//         uint128_t value1{35, 165};
//         EXPECT_EQ(uint128_t(0, 0), value1 << 277);
//         EXPECT_EQ(uint128_t(0, 0), Uint128_t::LeftShift(value1, 277));
//         EXPECT_EQ(uint128_t(0, 0), value1 >> 300);
//         EXPECT_EQ(uint128_t(0, 0), Uint128_t::RightShift(value1, 300));
//     }
// }
