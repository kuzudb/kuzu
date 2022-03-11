#include <string>

#include "include/gtest/gtest.h"

#include "src/common/include/exception.h"
#include "src/common/types/include/types_include.h"

using namespace graphflow::common;
using namespace std;

TEST(IntervalTests, FromCString) {
    // missing the specifier string (eg. days, minutes...)
    try {
        Interval::FromCString("3 years 2 months 45", strlen("3 years 2 months 45"));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // missing the number before the specifier string
    try {
        Interval::FromCString(
            "20 years 30 months 20 days minutes", strlen("20 years 30 months 20 days minutes"));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // numbers and specifier string must be separated by spaces
    try {
        Interval::FromCString(
            "2 years 30 minutes20 seconds", strlen("2 years 30 minutes20 seconds"));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // unrecognized specifier string (millseconds)
    try {
        Interval::FromCString("10 years 2 days 48 hours 28 seconds 12 millseconds",
            strlen("10 years 2 days 48 hours 28 seconds 12 millseconds"));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // multiple specifier strings
    try {
        Interval::FromCString("10 years 2 days 48 hours 28 seconds microseconds",
            strlen("10 years 2 days 48 hours 28 seconds microseconds"));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    interval_t result;

    result = Interval::FromCString("2 years 4 months 21 days 3 hours 4 minutes 28 us",
        strlen("2 years 4 months 21 days 3 hours 4 minutes 28 us"));
    EXPECT_EQ(result.months, 28);
    EXPECT_EQ(result.days, 21);
    EXPECT_EQ(result.micros, 11040000028);

    result = Interval::FromCString(
        "32 days 48 hours 12 minutes 280 us", strlen("32 days 48 hours 12 minutes 280 us"));
    EXPECT_EQ(result.months, 0);
    EXPECT_EQ(result.days, 32);
    EXPECT_EQ(result.micros, 173520000280);

    result = Interval::FromCString(
        "3 years 32 days     2 years 48 hours 12 minutes 280 us 52 days 32 months 20 minutes",
        strlen(
            "3 years 32 days     2 years 48 hours 12 minutes 280 us 52 days 32 months 20 minutes"));
    EXPECT_EQ(result.months, 92);
    EXPECT_EQ(result.days, 84);
    EXPECT_EQ(result.micros, 174720000280);
}

TEST(IntervalTests, toString) {
    string result;
    result = Interval::toString(
        Interval::FromCString("4 years 6 months 28 days 700 microseconds 20 seconds",
            strlen("4 years 6 months 28 days 700 microseconds 20 seconds")));
    EXPECT_EQ(result, "4 years 6 months 28 days 00:00:20.0007");

    result = Interval::toString(
        Interval::FromCString("3 hours 2 days 30 years 600 microseconds 200 seconds",
            strlen("3 hours 2 days 30 years 600 microseconds 200 seconds")));
    EXPECT_EQ(result, "30 years 2 days 03:03:20.0006");
}
