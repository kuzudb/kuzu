#include <cstdint>

#include "common/exception/overflow.h"
#include "common/exception/runtime.h"
#include "common/types/int128_t.h"
#include "common/types/uint128_t.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

TEST(Uint128Tests, Casting) {
    { // conversion from int32_t
        uint128_t value = 42;
        EXPECT_EQ(value.low, 42);
        EXPECT_EQ(value.high, 0);
    }

    { // conversion to int32_t
        uint128_t value = {(1ULL << 31) - 2, 0};
        int32_t int32_value = (int32_t)value;
        EXPECT_EQ(int32_value, (int32_t)((1ULL << 31) - 2));
    }

    { // conversion from int32_t (error: negative value)
        EXPECT_THROW({ [[maybe_unused]] uint128_t value = -64; }, OverflowException);
    }

    { // conversion to int64_t (narrow casting)
        uint128_t value = {2718, 1};
        int64_t int64_value = (int64_t)value;
        EXPECT_EQ(int64_value, 2718);
    }

    { // conversion to uint16_t (narrow casting)
        uint128_t value = {UINT16_MAX, UINT16_MAX};
        uint16_t uint16_value = (uint16_t)value;
        EXPECT_EQ(uint16_value, UINT16_MAX);
    }

    { // conversion from int8_t (error: negative value)
        int8_t int8_value = -64;
        EXPECT_THROW({ [[maybe_unused]] uint128_t value = int8_value; }, OverflowException);
    }

    { // conversion from int128_t 2^64 + 1 (no implicit casting from int128_t to uint128_t)
        int128_t int128_value{(uint64_t)1, (int64_t)1};
        uint128_t value = (uint128_t)int128_value;
        EXPECT_EQ(value.low, 1);
        EXPECT_EQ(value.high, 1);
    }

    { // conversion to int128_t
        int128_t int128_value = uint128_t{35, 165};
        EXPECT_EQ(int128_value.low, 35);
        EXPECT_EQ(int128_value.high, 165);
    }

    { // conversion from int128_t (error: negative value)
        int128_t negative_two = -2;
        EXPECT_THROW(
            { [[maybe_unused]] uint128_t value = (uint128_t)negative_two; }, OverflowException);
    }

    { // conversion to int128_t (error: overflow)
        uint128_t large_uint128 = {335882, (1ULL << 63) + 9101028};
        EXPECT_THROW(
            { [[maybe_unused]] int128_t value1 = (int128_t)large_uint128; }, OverflowException);
        uint128_t max_uint128 = {UINT64_MAX, UINT64_MAX};
        EXPECT_THROW(
            { [[maybe_unused]] int128_t value2 = (int128_t)max_uint128; }, OverflowException);
    }

    { // conversion to float and double
        uint128_t zero{0, 0};
        uint128_t value1{17, 1};
        uint128_t value2{0, (1ULL << 63)};           // 2^127
        uint128_t max_value{UINT64_MAX, UINT64_MAX}; // 2^128 - 1
        EXPECT_DOUBLE_EQ((double)zero, 0.0);
        EXPECT_FLOAT_EQ((float)zero, 0.0f);
        EXPECT_DOUBLE_EQ((double)value1, 18446744073709551633.0);
        EXPECT_FLOAT_EQ((float)value1, 18446744073709551633.0f);
        EXPECT_DOUBLE_EQ((double)value2, 170141183460469231731687303715884105728.0);
        EXPECT_FLOAT_EQ((float)value2, 170141183460469231731687303715884105728.0f);
        EXPECT_DOUBLE_EQ((double)max_value, 340282366920938463463374607431768211455.0);
    }

    { // conversion from float and double
        uint128_t zero = 0.0;
        uint128_t value1 = 1701411.188;
        EXPECT_EQ(zero, uint128_t(0, 0));
        EXPECT_EQ(value1, uint128_t(1701411, 0));
        uint128_t value2 = 3.14159265358979323846;
        EXPECT_EQ(value2, uint128_t(3, 0));
        uint128_t value3 = 2.71828182845904523536;
        EXPECT_EQ(value3, uint128_t(3, 0));
        uint128_t value4 = 176.5194f;
        EXPECT_EQ(value4, uint128_t(177, 0));
        uint128_t value5 = 20000000000000000000.142857f;
        uint128_t expected5{1553255926290448384, 1};
        int error5 = (int)(value5 > expected5 ? value5 - expected5 : expected5 - value5);
        EXPECT_TRUE(error5 < 500000000);
        EXPECT_THROW(
            {
                float negative_value = -17939777519457.96f;
                [[maybe_unused]] uint128_t value = negative_value;
            },
            OverflowException);
        EXPECT_THROW(
            {
                double overly_large_value = 1e39;
                [[maybe_unused]] uint128_t value = overly_large_value;
            },
            OverflowException);
    }

    { // tests for operator==
        EXPECT_EQ(350, uint128_t(350, 0));
        EXPECT_EQ((uint64_t)154250, uint128_t(154250, 0));
        EXPECT_EQ(uint128_t(12, 87), (uint128_t)(int128_t(12, 87)));
        EXPECT_NE(15, uint128_t(15, 1));
        EXPECT_NE(uint128_t(180, 179), uint128_t(181, 179));
    }
}

TEST(Uint128Tests, AdditionTest) {
    { // adding in upper bits: (1 << 64) + (2 << 64) = (3 << 64)
        uint128_t value1{0, 1};
        uint128_t value2{0, 2};
        uint128_t expected_value{0, 3};
        EXPECT_EQ(expected_value, value1 + value2);
        EXPECT_EQ(expected_value, UInt128_t::Add(value1, value2));
    }

    { // adding with overflow from lower to upper bits: (2^64 - 1) + 1 = (1 << 64)
        uint128_t value1{UINT64_MAX, 0};
        uint128_t value2{1, 0};
        uint128_t expected_value{0, 1};
        EXPECT_EQ(expected_value, value1 + value2);
        EXPECT_EQ(expected_value, UInt128_t::Add(value1, value2));
    }

    {                                            // (2^127 - 1) + 1 = 2^128
        uint128_t value1{UINT64_MAX, INT64_MAX}; // 01111111...111
        uint128_t value2{1, 0};
        uint128_t expected_value{0, (1ULL << 63)};
        EXPECT_EQ(expected_value, value1 + value2);
        EXPECT_EQ(expected_value, UInt128_t::Add(value1, value2));
    }

    {                                             // overflow
        uint128_t value1{UINT64_MAX, UINT64_MAX}; // 11111111...111
        uint128_t value2{1, 0};
        EXPECT_THROW(value1 + value2, OverflowException);
    }

    { // overflow 2
        uint128_t value1{100, 0xF000183A60873890};
        uint128_t value2{100, 0x0FFFE7C59F78CFFF};
        EXPECT_THROW(value1 + value2, OverflowException);
    }

    { // overflow 3 (edge case for UInt128_t::addInPlace)
        uint128_t value1{UINT64_MAX, 0};
        uint128_t value2{1, UINT64_MAX};
        EXPECT_THROW(value1 + value2, OverflowException);
        uint128_t value3{UINT64_MAX, 1};
        uint128_t value4{0, UINT64_MAX};
        EXPECT_THROW(value3 + value4, OverflowException);
    }
}

TEST(Uint128Tests, SubtractionTest) {
    { // subtracting in upper bits: (3 << 64) - (2 << 64) = (1 << 64)
        uint128_t value1{0, 3};
        uint128_t value2{0, 2};
        uint128_t expected_value{0, 1};
        EXPECT_EQ(expected_value, value1 - value2);
        EXPECT_EQ(expected_value, UInt128_t::Sub(value1, value2));
    }

    { // subtracting with underflow from upper to lower bits: (1 << 64) - 1 = (2^64 - 1)
        uint128_t value1{0, 1};
        uint128_t value2{1, 0};
        uint128_t expected_value{UINT64_MAX, 0};
        EXPECT_EQ(expected_value, value1 - value2);
        EXPECT_EQ(expected_value, UInt128_t::Sub(value1, value2));
    }

    {                                      // (2^127) - 1 = (2^127 - 1)
        uint128_t value1{0, (1ULL << 63)}; // 10000000...000
        uint128_t value2{1, 0};
        uint128_t expected_value{UINT64_MAX, INT64_MAX}; // 01111111...111
        EXPECT_EQ(expected_value, value1 - value2);
        EXPECT_EQ(expected_value, UInt128_t::Sub(value1, value2));
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
        EXPECT_EQ(expected_value, UInt128_t::Mul(value1, value2));
    }

    { // multiplying in upper bits: (1 << 64) * 3 = (3 << 64)
        uint128_t value1{0, 1};
        uint128_t value2{3, 0};
        uint128_t expected_value{0, 3};
        EXPECT_EQ(expected_value, value1 * value2);
        EXPECT_EQ(expected_value, UInt128_t::Mul(value1, value2));
    }

    { // multiplying with overflow from lower to upper bits: (2^64 - 1) * 2 = (2^65 - 2)
        uint128_t value1{UINT64_MAX, 0};
        uint128_t value2{2, 0};
        uint128_t expected_value{UINT64_MAX - 1, 1};
        EXPECT_EQ(expected_value, value1 * value2);
        EXPECT_EQ(expected_value, UInt128_t::Mul(value1, value2));
    }

    { // (2^64) * (2^63) = (2^127)
        uint128_t value1{0, 1};
        uint128_t value2{(1ULL << 63), 0};
        uint128_t expected_value{0, (1ULL << 63)};
        EXPECT_EQ(expected_value, value1 * value2);
        EXPECT_EQ(expected_value, UInt128_t::Mul(value1, value2));
    }

    {                                             // overflow
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
        EXPECT_EQ(expected_value, UInt128_t::Div(value1, value2));
    }

    { // integer division (truncating)
        uint128_t value1{1138, 0};
        uint128_t value2{3, 0};
        uint128_t expected_value{379, 0};
        EXPECT_EQ(expected_value, value1 / value2);
        EXPECT_EQ(expected_value, UInt128_t::Div(value1, value2));
    }

    {                                      // division in upper bits
        uint128_t value1{0, (1ULL << 63)}; // 100...000
        uint128_t value2{0, (1ULL << 62)}; // 010...000
        uint128_t expected_value{2, 0};
        EXPECT_EQ(expected_value, value1 / value2);
        EXPECT_EQ(expected_value, UInt128_t::Div(value1, value2));
    }

    { // division in upper bits with truncation
        uint128_t value1{3528930, (1ULL << 63) + (1ULL << 61)}; // 101...XXX
        uint128_t value2{0, (1ULL << 62)};                      // 010...000
        uint128_t expected_value{2, 0};
        EXPECT_EQ(expected_value, value1 / value2);
        EXPECT_EQ(expected_value, UInt128_t::Div(value1, value2));
    }

    {
        uint128_t value1{UINT64_MAX, UINT64_MAX};
        uint128_t value2{UINT64_MAX, UINT64_MAX};
        uint128_t expected_value{1, 0};
        EXPECT_EQ(expected_value, value1 / value2);
        EXPECT_EQ(expected_value, UInt128_t::Div(value1, value2));
    }

    {
        uint128_t value1{UINT64_MAX, INT64_MAX}; // 011...111
        uint128_t value2{0, (1ULL << 63)};       // 100...000
        uint128_t expected_value1{0, 0};
        EXPECT_EQ(expected_value1, value1 / value2);
        EXPECT_EQ(expected_value1, UInt128_t::Div(value1, value2));
        uint128_t expected_value2{1, 0};
        EXPECT_EQ(expected_value2, value2 / value1);
        EXPECT_EQ(expected_value2, UInt128_t::Div(value2, value1));
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
        EXPECT_EQ(expected_value, UInt128_t::Mod(value1, value2));
    }

    {
        uint128_t value1{1137, 0};
        uint128_t value2{3, 0};
        uint128_t expected_value{0, 0};
        EXPECT_EQ(expected_value, value1 % value2);
        EXPECT_EQ(expected_value, UInt128_t::Mod(value1, value2));
    }

    {
        uint128_t value1{UINT64_MAX, INT64_MAX}; // 011...111
        uint128_t value2{0, (1ULL << 63)};       // 100...000
        EXPECT_EQ(value1, value1 % value2);
        EXPECT_EQ(value1, UInt128_t::Mod(value1, value2));
        uint128_t expected_value{1, 0};
        EXPECT_EQ(expected_value, value2 % value1);
        EXPECT_EQ(expected_value, UInt128_t::Mod(value2, value1));
    }

    { // modulo by 0
        uint128_t value1{INT32_MAX, 73};
        uint128_t value2{0, 0};
        EXPECT_THROW(value1 % value2, RuntimeException);
    }
}

TEST(Uint128Tests, BitwiseTest1) {
    // small numbers
    uint128_t value1{0b10100000, 0b01111001};
    uint128_t value2{0b11000110, 0b10101011};

    {
        uint128_t expected_and{0b10000000, 0b00101001};
        EXPECT_EQ(expected_and, value1 & value2);
        EXPECT_EQ(expected_and, UInt128_t::BinaryAnd(value1, value2));
    }

    {
        uint128_t expected_or{0b11100110, 0b11111011};
        EXPECT_EQ(expected_or, value1 | value2);
        EXPECT_EQ(expected_or, UInt128_t::BinaryOr(value1, value2));
    }

    {
        uint128_t expected_xor{0b01100110, 0b11010010};
        EXPECT_EQ(expected_xor, value1 ^ value2);
        EXPECT_EQ(expected_xor, UInt128_t::Xor(value1, value2));
    }

    {
        uint128_t expected_not1{0xffffffffffffff00 + 0b01011111, 0xffffffffffffff00 + 0b10000110};
        uint128_t expected_not2{0xffffffffffffff00 + 0b00111001, 0xffffffffffffff00 + 0b01010100};
        EXPECT_EQ(expected_not1, ~value1);
        EXPECT_EQ(expected_not1, UInt128_t::BinaryNot(value1));
        EXPECT_EQ(expected_not2, ~value2);
        EXPECT_EQ(expected_not2, UInt128_t::BinaryNot(value2));
    }

    {
        EXPECT_EQ(value1, value1 << 0); // left shift by 0
        uint128_t expected_left_shift1{(uint64_t)(0b10100000) << 55, (uint64_t)(0b01111001) << 55};
        uint128_t expected_left_shift2{0, (1ULL << 63)};
        EXPECT_EQ(expected_left_shift1, value1 << 55);
        EXPECT_EQ(expected_left_shift1, UInt128_t::LeftShift(value1, 55));
        EXPECT_EQ(expected_left_shift2, value2 << 126);
        EXPECT_EQ(expected_left_shift2, UInt128_t::LeftShift(value2, 126));
    }
}

TEST(Uint128Tests, BitwiseTest2) {
    // large numbers (with most significant bit set)
    uint128_t value1{0, (1ULL << 63) + (1ULL << 62)}; // 11000...000
    uint128_t value2{2, (1ULL << 62) + (1ULL << 60)}; // 01010...010

    {
        uint128_t expected_and{0, (1ULL << 62)}; // 01000...000
        EXPECT_EQ(expected_and, value1 & value2);
        EXPECT_EQ(expected_and, UInt128_t::BinaryAnd(value1, value2));
    }

    {
        uint128_t expected_or{2, (1ULL << 63) + (1ULL << 62) + (1ULL << 60)}; // 11010...010
        EXPECT_EQ(expected_or, value1 | value2);
        EXPECT_EQ(expected_or, UInt128_t::BinaryOr(value1, value2));
    }

    {
        uint128_t expected_xor{2, (1ULL << 63) + (1ULL << 60)}; // 10010...010
        EXPECT_EQ(expected_xor, value1 ^ value2);
        EXPECT_EQ(expected_xor, UInt128_t::Xor(value1, value2));
    }

    {
        uint128_t expected_not1{UINT64_MAX, (1ULL << 62) - 1}; // 00111...111
        EXPECT_EQ(expected_not1, ~value1);
        EXPECT_EQ(expected_not1, UInt128_t::BinaryNot(value1));
    }

    {
        EXPECT_EQ(value1, value1 >> 0);                   // right shift by 0
        uint128_t expected_right_shift1{3, 0};            // 000...011
        uint128_t expected_right_shift2{(1ULL << 62), 1}; // 000...1 010... 0 (shift by 62)
        EXPECT_EQ(expected_right_shift1, value1 >> 126);
        EXPECT_EQ(expected_right_shift1, UInt128_t::RightShift(value1, 126));
        EXPECT_EQ(expected_right_shift2, value2 >> 62);
        EXPECT_EQ(expected_right_shift2, UInt128_t::RightShift(value2, 62));
    }
}

// Bit shifting by amounts that are > 127 or negative are currently undefined behavior.
// TEST(Uint128Tests, ExcessiveBitshift) {
//     {
//         uint128_t value1{35, 165};
//         EXPECT_EQ(uint128_t(0, 0), value1 << 277);
//         EXPECT_EQ(uint128_t(0, 0), UInt128_t::LeftShift(value1, 277));
//         EXPECT_EQ(uint128_t(0, 0), value1 >> 300);
//         EXPECT_EQ(uint128_t(0, 0), UInt128_t::RightShift(value1, 300));
//     }
// }

TEST(Uint128Tests, UnaryMinusTest) {
    {
        uint128_t zero{0, 0};
        EXPECT_EQ(-zero, uint128_t(0, 0));
        EXPECT_EQ(UInt128_t::negate(zero), uint128_t(0, 0));
        uint128_t one{1, 0};
        EXPECT_EQ(-one, uint128_t(UINT64_MAX, UINT64_MAX));
        EXPECT_EQ(UInt128_t::negate(one), uint128_t(UINT64_MAX, UINT64_MAX));
        EXPECT_EQ(-(uint128_t(16, 18)), uint128_t(UINT64_MAX - 15, UINT64_MAX - 18));
        EXPECT_EQ(-(uint128_t(15, 0)), uint128_t(UINT64_MAX - 14, UINT64_MAX));
        EXPECT_EQ(-(uint128_t(0, 15)), uint128_t(0, UINT64_MAX - 14));
        EXPECT_EQ(-(-(uint128_t(932, 654))), uint128_t(932, 654));
    }
}

TEST(Uint128Tests, InPlaceOperatorsTest) {
    {
        uint128_t value1{100, 34};
        uint128_t value2{0, 30};
        value1 += 7;
        EXPECT_EQ(value1, uint128_t(107, 34));
        value1 += value2;
        EXPECT_EQ(value1, uint128_t(107, 64));
        EXPECT_THROW({ value1 += uint128_t(UINT64_MAX, UINT64_MAX); }, OverflowException);
    }

    {
        uint128_t value1{15, 0};
        uint128_t value2{0, 2};
        value1 *= value2;
        EXPECT_EQ(value1, uint128_t(0, 30));
        value1 *= 12;
        EXPECT_EQ(value1, uint128_t(0, 360));
        EXPECT_THROW({ value1 *= uint128_t((1ULL << 61), 0); }, OverflowException);
    }
}

TEST(Uint128Tests, ComparisonOperatorsTest) {
    {
        uint128_t value1{3141, 2};
        uint128_t value2{0, 1};
        uint128_t value3{3141, 2};
        uint128_t value4{0, 0};
        EXPECT_TRUE(value1 == value3);
        EXPECT_FALSE(value1 == value2);
        EXPECT_TRUE(value2 != value3);
        EXPECT_FALSE(value1 != value3);
        EXPECT_TRUE(value2 < value1 && value1 > value2);
        EXPECT_FALSE(value1 > value3);
        EXPECT_FALSE(value1 < value3);
        EXPECT_TRUE(UINT64_MAX < value1);
        EXPECT_TRUE(value1 > 0 && value2 > 0 && value3 > 0);
        EXPECT_TRUE(value1 > value4 && value2 > value4 && value3 > value4);
        EXPECT_TRUE(value4 == 0);
        EXPECT_TRUE(value1 <= value3 && value1 >= value3);
        EXPECT_TRUE(value1 >= value2);
        EXPECT_FALSE(value2 >= value1);
    }
}

TEST(Uint128Tests, ToStringTest) {
    {
        uint128_t zero{0, 0};
        EXPECT_EQ("0", UInt128_t::toString(zero));
        uint128_t value1{0, 1}; // 2^64
        EXPECT_EQ("18446744073709551616", UInt128_t::toString(value1));
        uint128_t value2{0, (1ULL << 63)}; // 2^127
        EXPECT_EQ("170141183460469231731687303715884105728", UInt128_t::toString(value2));
        uint128_t value3{3141592653589793238ULL, 11082775389274182659ULL};
        EXPECT_EQ("204441121232347597927193748133362420182", UInt128_t::toString(value3));
        uint128_t max_value{UINT64_MAX, UINT64_MAX}; // 2^128 - 1
        EXPECT_EQ("340282366920938463463374607431768211455", UInt128_t::toString(max_value));
    }
}
