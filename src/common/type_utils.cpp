#include "common/type_utils.h"

#include "common/exception.h"
#include "common/string_utils.h"
#include "common/vector/value_vector.h"

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

std::string TypeUtils::listValueToString(
    const DataType& dataType, uint8_t* listValues, uint64_t pos) {
    switch (dataType.typeID) {
    case BOOL:
        return TypeUtils::toString(((bool*)listValues)[pos]);
    case INT64:
        return TypeUtils::toString(((int64_t*)listValues)[pos]);
    case DOUBLE:
        return TypeUtils::toString(((double_t*)listValues)[pos]);
    case DATE:
        return TypeUtils::toString(((date_t*)listValues)[pos]);
    case TIMESTAMP:
        return TypeUtils::toString(((timestamp_t*)listValues)[pos]);
    case INTERVAL:
        return TypeUtils::toString(((interval_t*)listValues)[pos]);
    case STRING:
        return TypeUtils::toString(((ku_string_t*)listValues)[pos]);
    case VAR_LIST:
        return TypeUtils::toString(((ku_list_t*)listValues)[pos], dataType);
    default:
        throw RuntimeException("Invalid data type " + Types::dataTypeToString(dataType) +
                               " for TypeUtils::listValueToString.");
    }
}

std::string TypeUtils::toString(const ku_list_t& val, const DataType& dataType) {
    std::string result = "[";
    for (auto i = 0u; i < val.size; ++i) {
        result += listValueToString(
            *dataType.getChildType(), reinterpret_cast<uint8_t*>(val.overflowPtr), i);
        result += (i == val.size - 1 ? "]" : ",");
    }
    return result;
}

std::string TypeUtils::toString(const list_entry_t& val, void* valVector) {
    auto listVector = (common::ValueVector*)valVector;
    std::string result = "[";
    auto values = ListVector::getListValues(listVector, val);
    for (auto i = 0u; i < val.size - 1; ++i) {
        result += (listVector->dataType.getChildType()->typeID == VAR_LIST ?
                          toString(reinterpret_cast<common::list_entry_t*>(values)[i],
                              ListVector::getDataVector(listVector)) :
                          listValueToString(*listVector->dataType.getChildType(), values, i)) +
                  ",";
    }
    result +=
        (listVector->dataType.getChildType()->typeID == VAR_LIST ?
                toString(reinterpret_cast<common::list_entry_t*>(values)[val.size - 1],
                    ListVector::getDataVector(listVector)) :
                listValueToString(*listVector->dataType.getChildType(), values, val.size - 1)) +
        "]";
    return result;
}

std::string TypeUtils::prefixConversionExceptionMessage(const char* data, DataTypeID dataTypeID) {
    return "Cannot convert string " + std::string(data) + " to " +
           Types::dataTypeToString(dataTypeID) + ".";
}

template<>
bool TypeUtils::isValueEqual(
    common::list_entry_t& leftEntry, common::list_entry_t& rightEntry, void* left, void* right) {
    auto leftVector = (ValueVector*)left;
    auto rightVector = (ValueVector*)right;
    if (leftVector->dataType != rightVector->dataType || leftEntry.size != rightEntry.size) {
        return false;
    }
    auto leftValues = ListVector::getListValues(leftVector, leftEntry);
    auto rightValues = ListVector::getListValues(rightVector, rightEntry);
    switch (leftVector->dataType.getChildType()->typeID) {
    case BOOL: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<uint8_t*>(leftValues)[i],
                    reinterpret_cast<uint8_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case INT64: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<int64_t*>(leftValues)[i],
                    reinterpret_cast<int64_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case DOUBLE: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<double_t*>(leftValues)[i],
                    reinterpret_cast<double_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case STRING: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<ku_string_t*>(leftValues)[i],
                    reinterpret_cast<ku_string_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case DATE: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<date_t*>(leftValues)[i],
                    reinterpret_cast<date_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case TIMESTAMP: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<timestamp_t*>(leftValues)[i],
                    reinterpret_cast<timestamp_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case INTERVAL: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<interval_t*>(leftValues)[i],
                    reinterpret_cast<interval_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case VAR_LIST: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<list_entry_t*>(leftValues)[i],
                    reinterpret_cast<list_entry_t*>(rightValues)[i],
                    ListVector::getDataVector(leftVector),
                    ListVector::getDataVector(rightVector))) {
                return false;
            }
        }
    } break;
    default: {
        throw RuntimeException("Unsupported data type " +
                               Types::dataTypeToString(leftVector->dataType) +
                               " for TypeUtils::isValueEqual.");
    }
    }
    return true;
}

} // namespace common
} // namespace kuzu
