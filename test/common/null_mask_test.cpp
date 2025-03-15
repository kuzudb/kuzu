#include <array>
#include <vector>

#include "common/null_mask.h"
#include "common/utils.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

TEST(NullMaskTests, TestRangeSingleEntry) {
    std::vector<uint64_t> data{NullMask::NO_NULL_ENTRY};
    NullMask::setNullRange(data.data(), 5, 5, true);
    ASSERT_EQ(data[0], 0b1111100000);

    data[0] = NullMask::ALL_NULL_ENTRY;
    NullMask::setNullRange(data.data(), 5, 5, false);
    ASSERT_EQ(data[0], ~0b1111100000);
}

TEST(NullMaskTests, TestRangeMultipleEntries1) {
    std::vector<uint64_t> data{NullMask::ALL_NULL_ENTRY, NullMask::ALL_NULL_ENTRY,
        NullMask::ALL_NULL_ENTRY};
    NullMask::setNullRange(data.data(), 5, 150, false);
    ASSERT_EQ(data[0], 0b11111);
    ASSERT_EQ(data[1], 0);
    ASSERT_NE(data[2], 0);
    ASSERT_TRUE(NullMask::isNull(data.data(), 4));
    ASSERT_FALSE(NullMask::isNull(data.data(), 5));
    ASSERT_FALSE(NullMask::isNull(data.data(), 154));
    ASSERT_TRUE(NullMask::isNull(data.data(), 155));
}

TEST(NullMaskTests, TestRangeMultipleEntries2) {
    std::vector<uint64_t> data{NullMask::NO_NULL_ENTRY, NullMask::NO_NULL_ENTRY,
        NullMask::NO_NULL_ENTRY};
    NullMask::setNullRange(data.data(), 5, 150, true);
    ASSERT_FALSE(NullMask::isNull(data.data(), 4));
    ASSERT_TRUE(NullMask::isNull(data.data(), 5));
    ASSERT_TRUE(NullMask::isNull(data.data(), 154));
    ASSERT_FALSE(NullMask::isNull(data.data(), 155));
}

TEST(NullMaskTests, CopyNullMaskAll) {
    const int size = 10;
    std::vector<uint64_t> source(size);
    source[0] = 0xffff;
    source[5] = 0xfbff;
    std::vector<uint64_t> dest(size);
    NullMask::copyNullMask(source.data(), 0, dest.data(), 0, size * 64);
    ASSERT_EQ(source, dest);
}

TEST(NullMaskTests, CopyNullMaskOffset) {
    const int size = 10;
    std::vector<uint64_t> source(size);
    source[0] = 0xffff;
    source[5] = 0xfbff;
    std::vector<uint64_t> dest(size);
    // Copy first word, except the first bit
    NullMask::copyNullMask(source.data(), 1, dest.data(), 1, 63);
    ASSERT_NE(source, dest);
    ASSERT_EQ(dest[0] & ~1, source[0] & ~1);
    ASSERT_EQ(dest[1] & 1, 0);

    // Copy word 4 and first bit of word 5
    NullMask::copyNullMask(source.data(), 4 * 64, dest.data(), 4 * 64, 65);
    // First bit should be set in destination
    ASSERT_EQ(dest[5] & 1, 1);
    // No other bit in word 5 should be set
    ASSERT_EQ(dest[5] & ~1, 0);
    // word 4 should be empty, like the source
    ASSERT_EQ(dest[4], 0);
}

TEST(NullMaskTests, CopyNullMaskSmall) {
    uint64_t src = 0;
    for (size_t bit = 0; bit < 64; bit++) {
        src = 1ull << bit;
        uint64_t dest = 0;
        NullMask::copyNullMask(&src, bit, &dest, bit, 1);
        EXPECT_EQ(src, dest) << "Failed copy for bit " << bit;
    }
    for (size_t bits = 0; bits < 64; bits++) {
        src = kuzu::common::BitmaskUtils::all1sMaskForLeastSignificantBits<uint64_t>(bits);
        uint64_t dest = 0;
        NullMask::copyNullMask(&src, 0, &dest, 0, bits);
        EXPECT_EQ(src, dest) << "Failed copy for first " << bits << " bits";
    }
}

TEST(NullMaskTests, CopyNullMaskOffsetInvert) {
    const int size = 10;
    std::vector<uint64_t> source(size);
    source[0] = 0xffff;
    source[5] = 0xfbff;
    std::vector<uint64_t> dest(size);
    // Copy first word, except the first bit, and invert copied bits
    NullMask::copyNullMask(source.data(), 1, dest.data(), 1, 63, true);
    ASSERT_NE(source, dest);
    // Dest word should match source word, with the exclusion of the first bit (but inverted)
    ASSERT_EQ(dest[0] & ~1, ~source[0] & ~1);
    ASSERT_EQ(dest[1] & 1, 0);

    // Copy word 4 and first bit of word 5
    NullMask::copyNullMask(source.data(), 4 * 64, dest.data(), 4 * 64, 65, true);
    // First bit should be set and inverted in destination
    ASSERT_EQ(dest[5] & 1, 0);
    // No other bit in word 5 should be set
    ASSERT_EQ(dest[5] & ~1, 0);
    // word 4 should be all set, as the inverted source
    ASSERT_EQ(dest[4], ~0);
}

TEST(NullMaskTests, CopyNullMaskReturnValue) {
    std::vector<uint64_t> emptySource(10, 0);
    std::vector<uint64_t> source(10, ~0ull);
    std::vector<uint64_t> dest(10);
    ASSERT_EQ(NullMask::copyNullMask(source.data(), 0, dest.data(), 0, 64, false /*invert*/), true);
    ASSERT_EQ(NullMask::copyNullMask(emptySource.data(), 0, dest.data(), 0, 64, false /*invert*/),
        false);

    ASSERT_EQ(NullMask::copyNullMask(source.data(), 0, dest.data(), 0, 64, true /*invert*/), false);
    ASSERT_EQ(NullMask::copyNullMask(emptySource.data(), 0, dest.data(), 0, 64, true /*invert*/),
        true);
}

TEST(NullMaskTests, ResizeNullMask) {
    NullMask mask(64);
    ASSERT_TRUE(!mask.isNull(63));
    mask.setNull(63, true);
    mask.resize(128);
    ASSERT_TRUE(mask.isNull(63));
    mask.setNull(127, true);
}

TEST(NullMaskTests, TestMinMax) {
    for (size_t i = 0; i < 63; i++) {
        uint64_t value = static_cast<uint64_t>(1) << i;
        {
            auto [min, max] = NullMask::getMinMax(&value, i, 1);
            EXPECT_TRUE(min) << "Min was incorrect for bit " << i;
            EXPECT_TRUE(max) << "Max was incorrect for bit " << i;
        }
        {
            auto [min, max] = NullMask::getMinMax(&value, 0, 64);
            EXPECT_FALSE(min);
            EXPECT_TRUE(max);
        }
    }
    {
        uint64_t value = ~0ull;
        auto [min, max] = NullMask::getMinMax(&value, 0, 64);
        EXPECT_TRUE(min);
        EXPECT_TRUE(max);
    }
    {
        std::array<uint64_t, 5> values = {0, 0, 0, 0, 0b111111111};
        auto [min, max] = NullMask::getMinMax(values.data(), 9, 64 * 4);
        EXPECT_FALSE(min);
        EXPECT_TRUE(max);
    }
    {
        std::array<uint64_t, 5> values = {~0ull, ~0ull, ~0ull, ~0ull, ~0b111111111ull};
        auto [min, max] = NullMask::getMinMax(values.data(), 9, 64 * 4);
        EXPECT_FALSE(min);
        EXPECT_TRUE(max);
    }
    {
        std::array<uint64_t, 5> values = {0, 0, 0, 0, 0};
        auto [min, max] = NullMask::getMinMax(values.data(), 23, 64 * 4);
        EXPECT_FALSE(min);
        EXPECT_FALSE(max);
    }
    {
        std::array<uint64_t, 5> values = {~0ull, ~0ull, ~0ull, ~0ull, ~0ull};
        auto [min, max] = NullMask::getMinMax(values.data(), 23, 64 * 4);
        EXPECT_TRUE(min);
        EXPECT_TRUE(max);
    }
}
