#include <string>

#include "common/types/int128_t.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

TEST(Int128Tests, LeftShiftTest) {
    {
        int128_t original_value{1ULL << 63, 0};
        int128_t expected_value{0, 1 << 2};
        EXPECT_EQ(expected_value, int128_t(1) << 66);
        EXPECT_EQ(expected_value, original_value << 3);
    }

    {
        int128_t original_value{-1};
        int128_t expected_value{-4};
        EXPECT_EQ(expected_value, original_value << 2);
    }

    {
        int128_t original_value{3, 55};

        static constexpr uint32_t shift_amount = 70;
        // the high bits will be discarded
        int128_t expected_value{0, 0b11 << (shift_amount - 64)};
        EXPECT_EQ(expected_value, int128_t(3) << shift_amount);
        EXPECT_EQ(expected_value, original_value << shift_amount);
    }

    {
        // ...11100
        int128_t original_value{-4};

        int128_t expected_value{0, -4};
        EXPECT_EQ(expected_value, original_value << 64);
    }
}

TEST(Int128Tests, RightShiftTest) {
    {
        int128_t original_value{0, 1 << 2};
        int128_t expected_value{1ULL << 63, 0};
        EXPECT_EQ(expected_value, original_value >> 3);
    }

    {
        int128_t original_value{0, -1};
        int128_t expected_value{-2};
        EXPECT_EQ(expected_value, original_value >> 63);
    }

    {
        int128_t original_value{3, 0b11 << 10};

        static constexpr uint32_t shift_amount = 70;
        int128_t expected_value{0b11 << 4}; // original low bits will be discarded
        EXPECT_EQ(expected_value, original_value >> shift_amount);
    }

    {
        // ...11100000
        int128_t original_value{0, -32};

        int128_t expected_value{-16};
        EXPECT_EQ(expected_value, original_value >> 65);
    }
}
