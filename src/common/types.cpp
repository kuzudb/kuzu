#include "src/common/include/types.h"

#include <cerrno>
#include <climits>
#include <cstdlib>
#include <stdexcept>

#include "src/common/include/date.h"
#include "src/common/include/exception.h"
#include "src/common/include/gf_string.h"
#include "src/common/include/interval.h"
#include "src/common/include/timestamp.h"
#include "src/common/include/value.h"

namespace graphflow {
namespace common {

DataType TypeUtils::getDataType(const std::string& dataTypeString) {
    if ("LABEL" == dataTypeString) {
        return LABEL;
    } else if ("NODE" == dataTypeString) {
        return NODE;
    } else if ("REL" == dataTypeString) {
        return REL;
    } else if ("INT64" == dataTypeString) {
        return INT64;
    } else if ("DOUBLE" == dataTypeString) {
        return DOUBLE;
    } else if ("BOOLEAN" == dataTypeString) {
        return BOOL;
    } else if ("STRING" == dataTypeString) {
        return STRING;
    } else if ("DATE" == dataTypeString) {
        return DATE;
    } else if ("TIMESTAMP" == dataTypeString) {
        return TIMESTAMP;
    } else if ("INTERVAL" == dataTypeString) {
        return INTERVAL;
    }
    throw invalid_argument("Cannot parse dataType: " + dataTypeString);
}

string TypeUtils::dataTypeToString(DataType dataType) {
    return DataTypeNames[dataType];
}

bool TypeUtils::isNumericalType(DataType dataType) {
    return dataType == INT64 || dataType == DOUBLE;
}

size_t TypeUtils::getDataTypeSize(DataType dataType) {
    switch (dataType) {
    case LABEL:
        return sizeof(label_t);
    case NODE:
        return sizeof(nodeID_t);
    case INT64:
        return sizeof(int64_t);
    case DOUBLE:
        return sizeof(double_t);
    case BOOL:
        return sizeof(uint8_t);
    case STRING:
        return sizeof(gf_string_t);
    case UNSTRUCTURED:
        return sizeof(Value);
    case DATE:
        return sizeof(date_t);
    case TIMESTAMP:
        return sizeof(timestamp_t);
    case INTERVAL:
        return sizeof(interval_t);
    default:
        throw invalid_argument(
            "Cannot infer the size of dataType: " + dataTypeToString(dataType) + ".");
    }
}

string TypeUtils::prefixConversionExceptionMessage(const char* data, DataType dataType) {
    return "Cannot convert string " + string(data) + " to " + dataTypeToString(dataType) + ".";
}

void TypeUtils::throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(
    const char* data, const char* eptr, const DataType dataType) {
    if (data == eptr) {
        throw ConversionException(prefixConversionExceptionMessage(data, dataType) +
                                  ". Invalid input. No characters consumed.");
    }
    if (*eptr != '\0') {
        throw ConversionException(prefixConversionExceptionMessage(data, dataType) +
                                  " Not all characters were read. read from character " + *data +
                                  " up to character: " + *eptr + ".");
    }
}

void TypeUtils::throwConversionExceptionOutOfRange(const char* data, const DataType dataType) {
    throw ConversionException(
        prefixConversionExceptionMessage(data, dataType) + " Input out of range.");
}

int64_t TypeUtils::convertToInt64(const char* data) {
    char* eptr;
    errno = 0;
    auto retVal = strtoll(data, &eptr, 10);
    throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(data, eptr, INT64);
    if ((LLONG_MAX == retVal || LLONG_MIN == retVal) && errno == ERANGE) {
        throw ConversionException(
            prefixConversionExceptionMessage(data, INT64) + " Input out of range.");
    }
    return retVal;
}

double_t TypeUtils::convertToDouble(const char* data) {
    char* eptr;
    errno = 0;
    auto retVal = strtod(data, &eptr);
    throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(data, eptr, DOUBLE);
    if ((HUGE_VAL == retVal || -HUGE_VAL == retVal) && errno == ERANGE) {
        throwConversionExceptionOutOfRange(data, DOUBLE);
    }
    return retVal;
};

uint8_t TypeUtils::convertToBoolean(const char* data) {
    auto len = strlen(data);
    if (len == 4 && 't' == tolower(data[0]) && 'r' == tolower(data[1]) && 'u' == tolower(data[2]) &&
        'e' == tolower(data[3])) {
        return TRUE;
    } else if (len == 5 && 'f' == tolower(data[0]) && 'a' == tolower(data[1]) &&
               'l' == tolower(data[2]) && 's' == tolower(data[3]) && 'e' == tolower(data[4])) {
        return FALSE;
    }
    throw ConversionException(
        prefixConversionExceptionMessage(data, BOOL) +
        ". Input is not equal to True or False (in a case-insensitive manner)");
}

Direction operator!(Direction& direction) {
    return (FWD == direction) ? BWD : FWD;
}

bool interval_t::operator>(const interval_t& rhs) const {
    return Interval::GreaterThan(*this, rhs);
}

interval_t interval_t::operator+(const interval_t& rhs) const {
    interval_t result{};
    result.months = months + rhs.months;
    result.days = days + rhs.days;
    result.micros = micros + rhs.micros;
    return result;
}

interval_t interval_t::operator-(const interval_t& rhs) const {
    interval_t result{};
    result.months = months - rhs.months;
    result.days = days - rhs.days;
    result.micros = micros - rhs.micros;
    return result;
}

interval_t interval_t::operator/(const uint64_t& rhs) const {
    interval_t result{};
    int32_t monthsRemainder = months % rhs;
    int32_t daysRemainder = (days + monthsRemainder * Interval::DAYS_PER_MONTH) % rhs;
    result.months = months / rhs;
    result.days = (days + monthsRemainder * Interval::DAYS_PER_MONTH) / rhs;
    result.micros = (micros + daysRemainder * Interval::MICROS_PER_DAY) / rhs;
    return result;
}

date_t date_t::operator+(const interval_t& interval) const {
    date_t result{};
    if (interval.months != 0) {
        int32_t year, month, day, maxDayInMonth;
        Date::Convert(*this, year, month, day);
        int32_t year_diff = interval.months / Interval::MONTHS_PER_YEAR;
        year += year_diff;
        month += interval.months - year_diff * Interval::MONTHS_PER_YEAR;
        if (month > Interval::MONTHS_PER_YEAR) {
            year++;
            month -= Interval::MONTHS_PER_YEAR;
        } else if (month <= 0) {
            year--;
            month += Interval::MONTHS_PER_YEAR;
        }
        // handle date overflow
        // example: 2020-01-31 + "1 months"
        maxDayInMonth = Date::MonthDays(year, month);
        day = day > maxDayInMonth ? maxDayInMonth : day;
        result = Date::FromDate(year, month, day);
    } else {
        result = *this;
    }
    if (interval.days != 0) {
        result.days += interval.days;
    }
    if (interval.micros != 0) {
        result.days += int32_t(interval.micros / Interval::MICROS_PER_DAY);
    }
    return result;
}

date_t date_t::operator-(const interval_t& interval) const {
    interval_t inverseRight{};
    inverseRight.months = -interval.months;
    inverseRight.days = -interval.days;
    inverseRight.micros = -interval.micros;
    return *this + inverseRight;
}

int64_t date_t::operator-(const date_t& rhs) const {
    return (*this).days - rhs.days;
}

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

} // namespace common
} // namespace graphflow
