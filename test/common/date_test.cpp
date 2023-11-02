#include "common/types/date_t.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

TEST(DateTests, IsLeapYearTest) {
    EXPECT_TRUE(Date::isLeapYear(2000));
    EXPECT_FALSE(Date::isLeapYear(2001));
}

TEST(DateTests, FromDateConvertGivesSame) {
    int32_t year, month, day;
    Date::convert(Date::fromDate(1909, 8, 28), year, month, day);
    EXPECT_EQ(1909, year);
    EXPECT_EQ(8, month);
    EXPECT_EQ(28, day);
}
