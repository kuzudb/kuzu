#include "src/common/include/type_utils.h"

#include <cerrno>
#include <climits>

#include "src/common/include/exception.h"
#include "src/common/types/include/value.h"

namespace graphflow {
namespace common {

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

bool TypeUtils::convertToBoolean(const char* data) {
    auto len = strlen(data);
    if (len == 4 && 't' == tolower(data[0]) && 'r' == tolower(data[1]) && 'u' == tolower(data[2]) &&
        'e' == tolower(data[3])) {
        return true;
    } else if (len == 5 && 'f' == tolower(data[0]) && 'a' == tolower(data[1]) &&
               'l' == tolower(data[2]) && 's' == tolower(data[3]) && 'e' == tolower(data[4])) {
        return false;
    }
    throw ConversionException(
        prefixConversionExceptionMessage(data, BOOL) +
        ". Input is not equal to True or False (in a case-insensitive manner)");
}

string TypeUtils::elementToString(DataType dataType, uint8_t* overflowPtr, uint64_t pos) {
    switch (dataType) {
    case BOOL:
        return TypeUtils::toString(((bool*)overflowPtr)[pos]);
    case INT64:
        return TypeUtils::toString(((int64_t*)overflowPtr)[pos]);
    case DOUBLE:
        return TypeUtils::toString(((double_t*)overflowPtr)[pos]);
    case DATE:
        return TypeUtils::toString(((date_t*)overflowPtr)[pos]);
    case TIMESTAMP:
        return TypeUtils::toString(((timestamp_t*)overflowPtr)[pos]);
    case INTERVAL:
        return TypeUtils::toString(((interval_t*)overflowPtr)[pos]);
    case STRING:
        return TypeUtils::toString(((gf_string_t*)overflowPtr)[pos]);
    default:
        assert(false);
    }
}

string TypeUtils::toString(const gf_list_t& val) {
    string result = "[";
    for (auto i = 0u; i < val.size - 1; ++i) {
        result +=
            elementToString(val.childType, reinterpret_cast<uint8_t*>(val.overflowPtr), i) + ",";
    }
    result +=
        elementToString(val.childType, reinterpret_cast<uint8_t*>(val.overflowPtr), val.size - 1) +
        "]";
    return result;
}

string TypeUtils::toString(const Literal& val) {
    switch (val.dataType) {
    case BOOL:
        return TypeUtils::toString(val.val.booleanVal);
    case INT64:
        return TypeUtils::toString(val.val.int64Val);
    case DOUBLE:
        return TypeUtils::toString(val.val.doubleVal);
    case NODE:
        return TypeUtils::toString(val.val.nodeID);
    case DATE:
        return TypeUtils::toString(val.val.dateVal);
    case TIMESTAMP:
        return TypeUtils::toString(val.val.timestampVal);
    case INTERVAL:
        return TypeUtils::toString(val.val.intervalVal);
    case STRING:
        return val.strVal;
    case LIST: {
        if (val.listVal.empty()) {
            return "[]";
        }
        string result = "[";
        for (auto i = 0u; i < val.listVal.size() - 1; i++) {
            result += toString(val.listVal[i]) + ",";
        }
        result += toString(val.listVal[val.listVal.size() - 1]);
        result += "]";
        return result;
    }
    default:
        assert(false);
    }
}

string TypeUtils::toString(const Value& val) {
    switch (val.dataType) {
    case BOOL:
        return TypeUtils::toString(val.val.booleanVal);
    case INT64:
        return TypeUtils::toString(val.val.int64Val);
    case DOUBLE:
        return TypeUtils::toString(val.val.doubleVal);
    case STRING:
        return TypeUtils::toString(val.val.strVal);
    case NODE:
        return TypeUtils::toString(val.val.nodeID);
    case DATE:
        return TypeUtils::toString(val.val.dateVal);
    case TIMESTAMP:
        return TypeUtils::toString(val.val.timestampVal);
    case INTERVAL:
        return TypeUtils::toString(val.val.intervalVal);
    case LIST:
        return TypeUtils::toString(val.val.listVal);
    default:
        assert(false);
    }
}

void TypeUtils::castLiteralToString(Literal& literal) {
    literal.strVal = toString(literal);
    literal.dataType = STRING;
}

void TypeUtils::copyString(
    const char* src, uint64_t len, gf_string_t& dest, OverflowBuffer& overflowBuffer) {
    TypeUtils::allocateSpaceForStringIfNecessary(dest, len, overflowBuffer);
    dest.set(src, len);
}

void TypeUtils::copyString(const string& src, gf_string_t& dest, OverflowBuffer& overflowBuffer) {
    TypeUtils::allocateSpaceForStringIfNecessary(dest, src.length(), overflowBuffer);
    dest.set(src);
}

void TypeUtils::copyString(
    const gf_string_t& src, gf_string_t& dest, OverflowBuffer& destOverflowBuffer) {
    TypeUtils::allocateSpaceForStringIfNecessary(dest, src.len, destOverflowBuffer);
    dest.set(src);
}

void TypeUtils::copyListValues(
    const uint8_t* srcValues, gf_list_t& dest, OverflowBuffer& overflowBuffer) {
    TypeUtils::allocateSpaceForList(
        dest, dest.size * Types::getDataTypeSize(dest.childType), overflowBuffer);
    dest.set(srcValues);
}

void TypeUtils::copyList(DataType childType, const vector<uint8_t*>& srcValues, gf_list_t& dest,
    OverflowBuffer& overflowBuffer) {
    TypeUtils::allocateSpaceForList(
        dest, srcValues.size() * Types::getDataTypeSize(childType), overflowBuffer);
    dest.set(childType, srcValues);
}

void TypeUtils::copyList(const gf_list_t& src, gf_list_t& dest, OverflowBuffer& overflowBuffer) {
    TypeUtils::allocateSpaceForList(
        dest, src.size * Types::getDataTypeSize(src.childType), overflowBuffer);
    dest.set(src);
    if (dest.childType == STRING) {
        for (auto i = 0u; i < dest.size; i++) {
            TypeUtils::copyString(((gf_string_t*)src.overflowPtr)[i],
                ((gf_string_t*)dest.overflowPtr)[i], overflowBuffer);
        }
    }
}

string TypeUtils::prefixConversionExceptionMessage(const char* data, DataType dataType) {
    return "Cannot convert string " + string(data) + " to " + Types::dataTypeToString(dataType) +
           ".";
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

} // namespace common
} // namespace graphflow
