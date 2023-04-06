#include "common/type_utils.h"

#include "common/exception.h"
#include "common/string_utils.h"

namespace kuzu {
namespace common {

uint32_t TypeUtils::convertToUint32(const char* data) {
    std::istringstream iss(data);
    uint32_t val;
    if (!(iss >> val)) {
        throw ConversionException(
            StringUtils::string_format("Failed to convert {} to uint32_t", data));
    }
    return val;
}

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

std::string TypeUtils::elementToString(
    const DataType& dataType, uint8_t* overflowPtr, uint64_t pos) {
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
        return TypeUtils::toString(((ku_string_t*)overflowPtr)[pos]);
    case VAR_LIST:
        return TypeUtils::toString(((ku_list_t*)overflowPtr)[pos], dataType);
    default:
        throw RuntimeException("Invalid data type " + Types::dataTypeToString(dataType) +
                               " for TypeUtils::elementToString.");
    }
}

std::string TypeUtils::toString(const ku_list_t& val, const DataType& dataType) {
    std::string result = "[";
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

std::string TypeUtils::prefixConversionExceptionMessage(const char* data, DataTypeID dataTypeID) {
    return "Cannot convert string " + std::string(data) + " to " +
           Types::dataTypeToString(dataTypeID) + ".";
}

} // namespace common
} // namespace kuzu
