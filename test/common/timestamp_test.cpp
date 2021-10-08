#include <string>

#include "include/gtest/gtest.h"

#include "src/common/include/date.h"
#include "src/common/include/exception.h"
#include "src/common/include/time.h"
#include "src/common/include/timestamp.h"

using namespace graphflow::common;
using namespace std;

TEST(TimestampTests, FromDatetime) {
    // day is out of range
    try {
        Timestamp::FromDatetime(Date::FromDate(1968, 12, 42), Time::FromTime(21, 32, 51));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // 2021 is not a leap year, February only has 28 days.
    try {
        Timestamp::FromDatetime(Date::FromDate(2021, 2, 29), Time::FromTime(21, 32, 51));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // hour is out of range
    try {
        Timestamp::FromDatetime(Date::FromDate(1968, 12, 22), Time::FromTime(25, 32, 51));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // second is out of range
    try {
        Timestamp::FromDatetime(Date::FromDate(2021, 2, 28), Time::FromTime(5, 52, 70));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // microsecond is out of rarnge
    try {
        Timestamp::FromDatetime(Date::FromDate(2021, 2, 28), Time::FromTime(5, 52, 42, 1000002));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    EXPECT_EQ(
        Timestamp::FromDatetime(Date::FromDate(2020, 10, 22), Time::FromTime(21, 32, 51)).value,
        1603402371000000);
    EXPECT_EQ(Timestamp::FromDatetime(Date::FromDate(1978, 5, 22), Time::FromTime(00, 54, 32, 7891))
                  .value,
        264646472007891);
}

TEST(TimestampTests, Convert) {
    date_t date;
    dtime_t time;

    // 1603402371000000 is 2020-10-22 21:32:51
    Timestamp::Convert(timestamp_t(1603402371000000), date, time);
    EXPECT_EQ(date.days, 18557);
    EXPECT_EQ(time.micros, 77571000000);

    // 4501210883005612 is 2112-08-21 08:21:23.005612
    Timestamp::Convert(timestamp_t(4501210883005612), date, time);
    EXPECT_EQ(date.days, 52097);
    EXPECT_EQ(time.micros, 30083005612);
}

TEST(TimestampTests, FromCString) {
    try {
        // missing day
        Timestamp::FromCString("2112-08 08:21:23.005612", strlen("2112-08 08:21:23.005612"));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    try {
        // missing second
        Timestamp::FromCString("2112-08-04 08:23.005612", strlen("2112-08-04 08:23.005612"));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    try {
        // missing a digit in day
        Timestamp::FromCString("2112-08-0", strlen("2112-08-0"));
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    EXPECT_EQ(
        Timestamp::FromCString("2112-08-21 08:21:23.005612", strlen("2112-08-21 08:21:23.005612"))
            .value,
        4501210883005612);
    EXPECT_EQ(Timestamp::FromCString("1992-04-28T09:22:56", strlen("1992-04-28T09:22:56")).value,
        704452976000000);
    EXPECT_EQ(Timestamp::FromCString("2023-08-12", strlen("2023-08-12")).value, 1691798400000000);
}

TEST(TimestampTests, toString) {
    EXPECT_EQ(Timestamp::toString(timestamp_t(1691798400000000)), "2023-08-12 00:00:00");
    EXPECT_EQ(Timestamp::toString(timestamp_t(4501210883005612)), "2112-08-21 08:21:23.005612");
    EXPECT_EQ(Timestamp::toString(timestamp_t(704452976000000)), "1992-04-28 09:22:56");
}
