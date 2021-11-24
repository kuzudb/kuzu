#include "src/common/include/types.h"

#include <cerrno>
#include <climits>
#include <cstdlib>
#include <stdexcept>

#include "src/common/include/exception.h"
#include "src/common/include/gf_string.h"
#include "src/common/include/interval.h"
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
    if (len == 0) {
        return NULL_BOOL;
    } else if (len == 4 && 't' == tolower(data[0]) && 'r' == tolower(data[1]) &&
               'u' == tolower(data[2]) && 'e' == tolower(data[3])) {
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

bool interval_t::operator<=(const interval_t& rhs) const {
    return !Interval::GreaterThan(*this, rhs);
}

bool interval_t::operator<(const interval_t& rhs) const {
    return !Interval::GreaterThanEquals(*this, rhs);
}

bool interval_t::operator>(const interval_t& rhs) const {
    return Interval::GreaterThan(*this, rhs);
}

bool interval_t::operator>=(const interval_t& rhs) const {
    return Interval::GreaterThanEquals(*this, rhs);
}

} // namespace common
} // namespace graphflow
