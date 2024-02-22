#include "common/exception/conversion.h"
#include "common/types/dtime_t.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

TEST(TimeTests, FromTime) {
    // Hour out of range
    try {
        Time::fromTime(25, 24, 32, 231321);
        FAIL();
    } catch (ConversionException& e) {
        ASSERT_STREQ(
            e.what(), "Conversion exception: Time field value out of range: 25:24:32[.231321].");
    } catch (std::exception& e) { FAIL(); }

    // Minute out of range
    try {
        Time::fromTime(21, 60, 22, 212322);
        FAIL();
    } catch (ConversionException& e) {
        ASSERT_STREQ(
            e.what(), "Conversion exception: Time field value out of range: 21:60:22[.212322].");
    } catch (std::exception& e) { FAIL(); }

    // Second out of range
    try {
        Time::fromTime(14, 22, 61);
        FAIL();
    } catch (ConversionException& e) {
        ASSERT_STREQ(
            e.what(), "Conversion exception: Time field value out of range: 14:22:61[.0].");
    } catch (std::exception& e) { FAIL(); }

    // Microsecond out of range
    try {
        Time::fromTime(14, 22, 42, 1000001);
        FAIL();
    } catch (ConversionException& e) {
        ASSERT_STREQ(
            e.what(), "Conversion exception: Time field value out of range: 14:22:42[.1000001].");
    } catch (std::exception& e) { FAIL(); }

    EXPECT_EQ(80052000000, Time::fromTime(22, 14, 12).micros);
    EXPECT_EQ(58991000024, Time::fromTime(16, 23, 11, 24).micros);
}

TEST(TimeTests, FromCString) {
    // Hour out of range
    try {
        Time::fromCString("25:12:00", strlen("25:12:00"));
        FAIL();
    } catch (ConversionException& e) {
        ASSERT_STREQ(e.what(), "Conversion exception: Error occurred during parsing time. Given: "
                               "\"25:12:00\". Expected format: (hh:mm:ss[.zzzzzz]).");
    } catch (std::exception& e) { FAIL(); }

    // Hour out of range
    try {
        Time::fromCString("24:00:00", strlen("24:00:00"));
        FAIL();
    } catch (ConversionException& e) {
        ASSERT_STREQ(e.what(), "Conversion exception: Error occurred during parsing time. Given: "
                               "\"24:00:00\". Expected format: (hh:mm:ss[.zzzzzz]).");
    } catch (std::exception& e) { FAIL(); }

    EXPECT_EQ(83531000000, Time::fromCString("23:12:11", strlen("23:12:11")).micros);
    EXPECT_EQ(46792122311, Time::fromCString("12:59:52.122311", strlen("12:59:52.122311")).micros);
    EXPECT_EQ(11561200, Time::fromCString("00:00:11.5612", strlen("00:00:11.5612")).micros);
    EXPECT_EQ(11561200, Time::fromCString(" 00:00:11.5612", strlen(" 00:00:11.5612")).micros);
}

TEST(TimeTests, Convert) {
    // 46792122311 micro seconds from 1970-01-01, which is 12:59:52.122311
    int32_t hour, minute, second, microsecond = 0;
    Time::convert(dtime_t(46792122311), hour, minute, second, microsecond);
    EXPECT_EQ(hour, 12);
    EXPECT_EQ(minute, 59);
    EXPECT_EQ(second, 52);
    EXPECT_EQ(microsecond, 122311);
}

TEST(TimeTests, toString) {
    EXPECT_EQ(Time::toString(dtime_t(79140562100)), "21:59:00.5621");
    EXPECT_EQ(Time::toString(dtime_t(48179000000)), "13:22:59");
    EXPECT_EQ(Time::toString(dtime_t(0)), "00:00:00");
}
