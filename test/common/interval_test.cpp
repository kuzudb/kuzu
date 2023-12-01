#include <string>

#include "common/types/interval_t.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

TEST(IntervalTests, FromCString) {
    interval_t result;
    result = Interval::fromCString("2 years 4 months 21 days 3 hours 4 minutes 28 us",
        strlen("2 years 4 months 21 days 3 hours 4 minutes 28 us"));
    EXPECT_EQ(result.months, 28);
    EXPECT_EQ(result.days, 21);
    EXPECT_EQ(result.micros, 11040000028);
    result = Interval::fromCString(
        "32 days 48 hours 12 minutes 280 us", strlen("32 days 48 hours 12 minutes 280 us"));
    EXPECT_EQ(result.months, 0);
    EXPECT_EQ(result.days, 32);
    EXPECT_EQ(result.micros, 173520000280);
    result = Interval::fromCString(
        "3 years 32 days     2 years 48 hours 12 minutes 280 us 52 days 32 months 20 minutes",
        strlen(
            "3 years 32 days     2 years 48 hours 12 minutes 280 us 52 days 32 months 20 minutes"));
    EXPECT_EQ(result.months, 92);
    EXPECT_EQ(result.days, 84);
    EXPECT_EQ(result.micros, 174720000280);
}

TEST(IntervalTests, toString) {
    std::string result;
    result = Interval::toString(
        Interval::fromCString("4 years 6 months 28 days 700 microseconds 20 seconds",
            strlen("4 years 6 months 28 days 700 microseconds 20 seconds")));
    EXPECT_EQ(result, "4 years 6 months 28 days 00:00:20.0007");

    result = Interval::toString(
        Interval::fromCString("3 hours 2 days 30 years 600 microseconds 200 seconds",
            strlen("3 hours 2 days 30 years 600 microseconds 200 seconds")));
    EXPECT_EQ(result, "30 years 2 days 03:03:20.0006");
}
