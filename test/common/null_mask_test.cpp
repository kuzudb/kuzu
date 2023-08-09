#include "common/null_mask.h"
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

TEST(NullMaskTests, TestRangeMultipleEntries) {
    std::vector<uint64_t> data{
        NullMask::ALL_NULL_ENTRY, NullMask::ALL_NULL_ENTRY, NullMask::ALL_NULL_ENTRY};
    NullMask::setNullRange(data.data(), 5, 150, false);
    ASSERT_EQ(data[0], 0b11111);
    ASSERT_EQ(data[1], 0);
    ASSERT_FALSE(NullMask::isNull(data.data(), 154));
    ASSERT_TRUE(NullMask::isNull(data.data(), 155));
}
