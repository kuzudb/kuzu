#include <string>

#include "include/gtest/gtest.h"

#include "src/common/include/date.h"
#include "src/common/include/exception.h"

using namespace graphflow::common;
using namespace std;

TEST(DateTests, IsLeapYearTest) {
    EXPECT_TRUE(Date::IsLeapYear(2000));
    EXPECT_FALSE(Date::IsLeapYear(2001));
}

TEST(DateTests, FromDate) {
    // Year out of range
    try {
        date_t result = Date::FromDate(110000, 11, 42);
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }
    // Month out of range
    try {
        date_t result = Date::FromDate(2000, 15, 22);
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }
    // Day out of range
    try {
        date_t result = Date::FromDate(2000, 11, 42);
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    EXPECT_EQ(0, Date::FromDate(1970, 1, 1).days);
    EXPECT_EQ(7, Date::FromDate(1970, 1, 8).days);
    EXPECT_EQ(-12, Date::FromDate(1969, 12, 20).days);
}

TEST(DateTests, FromDateConvertGivesSame) {
    int32_t year, month, day;
    Date::Convert(Date::FromDate(1909, 8, 28), year, month, day);
    EXPECT_EQ(1909, year);
    EXPECT_EQ(8, month);
    EXPECT_EQ(28, day);
}
