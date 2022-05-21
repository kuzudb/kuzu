#include "src/common/include/type_utils.h"

#include <cerrno>
#include <climits>

#include "src/common/include/exception.h"
#include "src/common/include/utils.h"
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

uint32_t TypeUtils::convertToUint32(const char* data) {
    istringstream iss(data);
    uint32_t val;
    if (!(iss >> val)) {
        throw ConversionException(
            StringUtils::string_format("Failed to convert %s to uint32_t", data));
    }
    return val;
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

string TypeUtils::elementToString(const DataType& dataType, uint8_t* overflowPtr, uint64_t pos) {
    switch (dataType.typeID) {
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
    case LIST:
        return TypeUtils::toString(((gf_list_t*)overflowPtr)[pos], dataType);
    default:
        assert(false);
    }
}

string TypeUtils::toString(const gf_list_t& val, const DataType& dataType) {
    string result = "[";
    for (auto i = 0u; i < val.size - 1; ++i) {
        result +=
            elementToString(*dataType.childType, reinterpret_cast<uint8_t*>(val.overflowPtr), i) +
            ",";
    }
    result += elementToString(
                  *dataType.childType, reinterpret_cast<uint8_t*>(val.overflowPtr), val.size - 1) +
              "]";
    return result;
}

string TypeUtils::toString(const Literal& literal) {
    if (literal.isNull()) {
        return "NULL";
    }
    switch (literal.dataType.typeID) {
    case BOOL:
        return TypeUtils::toString(literal.val.booleanVal);
    case INT64:
        return TypeUtils::toString(literal.val.int64Val);
    case DOUBLE:
        return TypeUtils::toString(literal.val.doubleVal);
    case NODE:
        return TypeUtils::toString(literal.val.nodeID);
    case DATE:
        return TypeUtils::toString(literal.val.dateVal);
    case TIMESTAMP:
        return TypeUtils::toString(literal.val.timestampVal);
    case INTERVAL:
        return TypeUtils::toString(literal.val.intervalVal);
    case STRING:
        return literal.strVal;
    case LIST: {
        if (literal.listVal.empty()) {
            return "[]";
        }
        string result = "[";
        for (auto i = 0u; i < literal.listVal.size() - 1; i++) {
            result += toString(literal.listVal[i]) + ",";
        }
        result += toString(literal.listVal[literal.listVal.size() - 1]);
        result += "]";
        return result;
    }
    default:
        assert(false);
    }
}

string TypeUtils::toString(const Value& val) {
    switch (val.dataType.typeID) {
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
        return TypeUtils::toString(val.val.listVal, val.dataType);
    default:
        assert(false);
    }
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

void TypeUtils::copyListNonRecursive(const uint8_t* srcValues, gf_list_t& dest,
    const DataType& dataType, OverflowBuffer& overflowBuffer) {
    TypeUtils::allocateSpaceForList(
        dest, dest.size * Types::getDataTypeSize(*dataType.childType), overflowBuffer);
    dest.set(srcValues, dataType);
}

void TypeUtils::copyListNonRecursive(const vector<uint8_t*>& srcValues, gf_list_t& dest,
    const DataType& dataType, OverflowBuffer& overflowBuffer) {
    TypeUtils::allocateSpaceForList(
        dest, srcValues.size() * Types::getDataTypeSize(*dataType.childType), overflowBuffer);
    dest.set(srcValues, dataType.childType->typeID);
}

void TypeUtils::copyListRecursiveIfNested(const gf_list_t& src, gf_list_t& dest,
    const DataType& dataType, OverflowBuffer& overflowBuffer, uint32_t srcStartIdx,
    uint32_t srcEndIdx) {
    if (srcEndIdx == UINT32_MAX) {
        srcEndIdx = src.size - 1;
    }
    assert(srcEndIdx < src.size);
    auto numElements = srcEndIdx - srcStartIdx + 1;
    auto elementSize = Types::getDataTypeSize(*dataType.childType);
    TypeUtils::allocateSpaceForList(dest, numElements * elementSize, overflowBuffer);
    memcpy((uint8_t*)dest.overflowPtr, (uint8_t*)src.overflowPtr + srcStartIdx * elementSize,
        numElements * elementSize);
    dest.size = numElements;
    if (dataType.childType->typeID == STRING) {
        for (auto i = 0u; i < dest.size; i++) {
            TypeUtils::copyString(((gf_string_t*)src.overflowPtr)[i + srcStartIdx],
                ((gf_string_t*)dest.overflowPtr)[i], overflowBuffer);
        }
    }
    if (dataType.childType->typeID == LIST) {
        for (auto i = 0u; i < dest.size; i++) {
            TypeUtils::copyListRecursiveIfNested(((gf_list_t*)src.overflowPtr)[i + srcStartIdx],
                ((gf_list_t*)dest.overflowPtr)[i], *dataType.childType, overflowBuffer);
        }
    }
}

string TypeUtils::prefixConversionExceptionMessage(const char* data, DataTypeID dataTypeID) {
    return "Cannot convert string " + string(data) + " to " + Types::dataTypeToString(dataTypeID) +
           ".";
}

void TypeUtils::throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(
    const char* data, const char* eptr, DataTypeID dataTypeID) {
    if (data == eptr) {
        throw ConversionException(prefixConversionExceptionMessage(data, dataTypeID) +
                                  ". Invalid input. No characters consumed.");
    }
    if (*eptr != '\0') {
        throw ConversionException(prefixConversionExceptionMessage(data, dataTypeID) +
                                  " Not all characters were read. read from character " + *data +
                                  " up to character: " + *eptr + ".");
    }
}

void TypeUtils::throwConversionExceptionOutOfRange(const char* data, DataTypeID dataTypeID) {
    throw ConversionException(
        prefixConversionExceptionMessage(data, dataTypeID) + " Input out of range.");
}

} // namespace common
} // namespace graphflow
