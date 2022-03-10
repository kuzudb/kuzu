#include "include/timestamp_t.h"

#include "include/cast_helpers.h"

#include "src/common/include/exception.h"

namespace graphflow {
namespace common {

timestamp_t timestamp_t::operator+(const interval_t& interval) const {
    date_t date{};
    date_t result_date{};
    dtime_t time{};
    Timestamp::Convert(*this, date, time);
    result_date = date + interval;
    date = result_date;
    int64_t diff =
        interval.micros - ((interval.micros / Interval::MICROS_PER_DAY) * Interval::MICROS_PER_DAY);
    time.micros += diff;
    if (time.micros >= Interval::MICROS_PER_DAY) {
        time.micros -= Interval::MICROS_PER_DAY;
        date.days++;
    } else if (time.micros < 0) {
        time.micros += Interval::MICROS_PER_DAY;
        date.days--;
    }
    return Timestamp::FromDatetime(date, time);
}

timestamp_t timestamp_t::operator-(const interval_t& interval) const {
    interval_t inverseRight{};
    inverseRight.months = -interval.months;
    inverseRight.days = -interval.days;
    inverseRight.micros = -interval.micros;
    return (*this) + inverseRight;
}

interval_t timestamp_t::operator-(const timestamp_t& rhs) const {
    interval_t result{};
    uint64_t diff = abs(value - rhs.value);
    result.months = 0;
    result.days = diff / Interval::MICROS_PER_DAY;
    result.micros = diff % Interval::MICROS_PER_DAY;
    if (value < rhs.value) {
        result.days = -result.days;
        result.micros = -result.micros;
    }
    return result;
}

static_assert(sizeof(timestamp_t) == sizeof(int64_t), "timestamp_t was padded");

// string format is YYYY-MM-DDThh:mm:ss[.mmmmmm]
// T may be a space, timezone is not supported yet
// ISO 8601
timestamp_t Timestamp::FromCString(const char* str, uint64_t len) {
    timestamp_t result;
    uint64_t pos;
    date_t date;
    dtime_t time;

    // Find the string len for date
    uint32_t dateStrLen = 0;
    while (dateStrLen < len && str[dateStrLen] != ' ' && str[dateStrLen] != 'T') {
        dateStrLen++;
    }

    if (!Date::TryConvertDate(str, dateStrLen, pos, date)) {
        throw ConversionException("Error occurred during parsing time Given: \"" +
                                  string(str, len) +
                                  "\"Expected Format: (YYYY-MM-DD HH:MM:SS[.MS])");
    }
    if (pos == len) {
        // no time: only a date
        result = FromDatetime(date, dtime_t(0));
        return result;
    }
    // try to parse a time field
    if (str[pos] == ' ' || str[pos] == 'T') {
        pos++;
    }
    uint64_t time_pos = 0;
    if (!Time::TryConvertTime(str + pos, len - pos, time_pos, time)) {
        throw ConversionException("Error occurred during parsing time Given: \"" +
                                  string(str, len) +
                                  "\"Expected Format: (YYYY-MM-DD HH:MM:SS[.MS])");
    }
    pos += time_pos;
    result = FromDatetime(date, time);

    // skip any spaces at the end
    while (pos < len && isspace(str[pos])) {
        pos++;
    }
    if (pos < len) {
        throw ConversionException(string(str, len));
    }
    return result;
}

string Timestamp::toString(timestamp_t timestamp) {
    date_t date;
    dtime_t time;
    Timestamp::Convert(timestamp, date, time);
    return Date::toString(date) + " " + Time::toString(time);
}

date_t Timestamp::GetDate(timestamp_t timestamp) {
    return date_t((timestamp.value + (timestamp.value < 0)) / Interval::MICROS_PER_DAY -
                  (timestamp.value < 0));
}

dtime_t Timestamp::GetTime(timestamp_t timestamp) {
    date_t date = Timestamp::GetDate(timestamp);
    return dtime_t(timestamp.value - (int64_t(date.days) * int64_t(Interval::MICROS_PER_DAY)));
}

timestamp_t Timestamp::FromDatetime(date_t date, dtime_t time) {
    timestamp_t result;
    int32_t year, month, day, hour, minute, second, microsecond = -1;
    Date::Convert(date, year, month, day);
    Time::Convert(time, hour, minute, second, microsecond);
    if (!Date::IsValid(year, month, day) || !Time::IsValid(hour, minute, second, microsecond)) {
        throw ConversionException("Invalid date or time format");
    }
    result.value = date.days * Interval::MICROS_PER_DAY + time.micros;
    return result;
}

void Timestamp::Convert(timestamp_t timestamp, date_t& out_date, dtime_t& out_time) {
    out_date = GetDate(timestamp);
    out_time = GetTime(timestamp);
}

} // namespace common
} // namespace graphflow
