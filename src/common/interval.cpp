#include "src/common/include/interval.h"

#include "src/common/include/cast_helpers.h"
#include "src/common/include/exception.h"
#include "src/common/include/utils.h"

using namespace std;

namespace graphflow {
namespace common {

void Interval::addition(interval_t& result, uint64_t number, string specifierStr) {
    StringUtils::toLower(specifierStr);
    if (specifierStr == "year" || specifierStr == "years" || specifierStr == "y") {
        result.months += number * MONTHS_PER_YEAR;
    } else if (specifierStr == "month" || specifierStr == "months" || specifierStr == "mon") {
        result.months += number;
    } else if (specifierStr == "day" || specifierStr == "days" || specifierStr == "d") {
        result.days += number;
    } else if (specifierStr == "hour" || specifierStr == "hours" || specifierStr == "h") {
        result.micros += number * MICROS_PER_HOUR;
    } else if (specifierStr == "minute" || specifierStr == "minutes" || specifierStr == "m") {
        result.micros += number * MICROS_PER_MINUTE;
    } else if (specifierStr == "second" || specifierStr == "seconds" || specifierStr == "s") {
        result.micros += number * MICROS_PER_SEC;
    } else if (specifierStr == "millisecond" || specifierStr == "milliseconds" ||
               specifierStr == "ms" || specifierStr == "msec") {
        result.micros += number * MICROS_PER_MSEC;
    } else if (specifierStr == "microsecond" || specifierStr == "microseconds" ||
               specifierStr == "us") {
        result.micros += number;
    } else {
        throw ConversionException("Unrecognized interval specifier string: " + specifierStr);
    }
}

void Interval::parseIntervalField(string buf, uint64_t& pos, uint64_t len, interval_t& result) {
    uint64_t number;
    uint64_t offset = 0;
    // parse digits
    number = stoi(buf.c_str() + pos, reinterpret_cast<size_t*>(&offset));
    pos += offset;
    // skip spaces
    while (pos < len && isspace(buf[pos])) {
        pos++;
    }
    if (pos == len) {
        throw ConversionException(buf + " is not in a correct format");
    }
    // Parse intervalPartSpecifier (eg. hours, dates, minutes)
    uint64_t spacePos = string(buf).find(' ', pos);
    if (spacePos == string::npos) {
        spacePos = len;
    }
    string specifierStr = buf.substr(pos, spacePos - pos);
    pos = spacePos;
    addition(result, number, specifierStr);
}

interval_t Interval::FromCString(const char* gf_str, uint64_t len) {
    string str = string(gf_str, len);
    interval_t result;
    uint64_t pos = 0;
    result.days = 0;
    result.micros = 0;
    result.months = 0;

    if (str[pos] == '@') {
        pos++;
    }

    while (pos < len) {
        if (isdigit(str[pos])) {
            parseIntervalField(str, pos, len, result);
        } else if (!isspace(str[pos])) {
            throw ConversionException(str + " is not in a correct format");
        }
        pos++;
    }
    return result;
}

string Interval::toString(interval_t interval) {
    char buffer[70];
    uint64_t length = IntervalToStringCast::Format(interval, buffer);
    return string(buffer, length);
}

// helper function of interval comparison
void Interval::NormalizeIntervalEntries(
    interval_t input, int64_t& months, int64_t& days, int64_t& micros) {
    int64_t extra_months_d = input.days / Interval::DAYS_PER_MONTH;
    int64_t extra_months_micros = input.micros / Interval::MICROS_PER_MONTH;
    input.days -= extra_months_d * Interval::DAYS_PER_MONTH;
    input.micros -= extra_months_micros * Interval::MICROS_PER_MONTH;

    int64_t extra_days_micros = input.micros / Interval::MICROS_PER_DAY;
    input.micros -= extra_days_micros * Interval::MICROS_PER_DAY;

    months = input.months + extra_months_d + extra_months_micros;
    days = input.days + extra_days_micros;
    micros = input.micros;
}

bool Interval::GreaterThan(const interval_t& left, const interval_t& right) {
    int64_t lMonths, lDays, lMicros;
    int64_t rMonths, rDays, rMicros;
    NormalizeIntervalEntries(left, lMonths, lDays, lMicros);
    NormalizeIntervalEntries(right, rMonths, rDays, rMicros);
    if (lMonths > rMonths) {
        return true;
    } else if (lMonths < rMonths) {
        return false;
    }
    if (lDays > rDays) {
        return true;
    } else if (lDays < rDays) {
        return false;
    }
    return lMicros > rMicros;
}

} // namespace common
} // namespace graphflow
