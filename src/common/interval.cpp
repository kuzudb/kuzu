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

void Interval::parseIntervalField(
    const char* buf, uint64_t& pos, uint64_t len, interval_t& result) {
    uint64_t number;
    uint64_t offset = 0;
    // parse digits
    number = stoi(buf + pos, &offset);
    pos += offset;
    // skip spaces
    while (pos < len && isspace(buf[pos])) {
        pos++;
    }
    if (pos == len) {
        throw ConversionException(string(buf, len) + " is not in a correct format");
    }
    // Parse intervalPartSpecifier (eg. hours, dates, minutes)
    uint64_t spacePos = string(buf).find(' ', pos);
    if (spacePos == string::npos) {
        spacePos = len;
    }
    string specifierStr = string(buf).substr(pos, spacePos - pos);
    pos = spacePos;
    addition(result, number, specifierStr);
}

interval_t Interval::FromCString(const char* str, uint64_t len) {
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
            throw ConversionException(string(str, len) + " is not in a correct format");
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

} // namespace common
} // namespace graphflow
