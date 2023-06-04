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
        prefixConversionExceptionMessage(data, LogicalTypeID::BOOL) +
        ". Input is not equal to True or False (in a case-insensitive manner)");
}

std::string TypeUtils::castValueToString(
    const LogicalType& dataType, uint8_t* value, void* vector) {
    auto valueVector = reinterpret_cast<ValueVector*>(vector);
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return TypeUtils::toString(*reinterpret_cast<bool*>(value));
    case LogicalTypeID::INT64:
        return TypeUtils::toString(*reinterpret_cast<int64_t*>(value));
    case LogicalTypeID::INT32:
        return TypeUtils::toString(*reinterpret_cast<int32_t*>(value));
    case LogicalTypeID::INT16:
        return TypeUtils::toString(*reinterpret_cast<int16_t*>(value));
    case LogicalTypeID::DOUBLE:
        return TypeUtils::toString(*reinterpret_cast<double_t*>(value));
    case LogicalTypeID::FLOAT:
        return TypeUtils::toString(*reinterpret_cast<float_t*>(value));
    case LogicalTypeID::DATE:
        return TypeUtils::toString(*reinterpret_cast<date_t*>(value));
    case LogicalTypeID::TIMESTAMP:
        return TypeUtils::toString(*reinterpret_cast<timestamp_t*>(value));
    case LogicalTypeID::INTERVAL:
        return TypeUtils::toString(*reinterpret_cast<interval_t*>(value));
    case LogicalTypeID::STRING:
        return TypeUtils::toString(*reinterpret_cast<ku_string_t*>(value));
    case LogicalTypeID::INTERNAL_ID:
        return TypeUtils::toString(*reinterpret_cast<internalID_t*>(value));
    case LogicalTypeID::VAR_LIST:
        return TypeUtils::toString(*reinterpret_cast<list_entry_t*>(value), valueVector);
    case LogicalTypeID::STRUCT:
        return TypeUtils::toString(*reinterpret_cast<struct_entry_t*>(value), valueVector);
    default:
        throw RuntimeException("Invalid data type " + LogicalTypeUtils::dataTypeToString(dataType) +
                               " for TypeUtils::castValueToString.");
    }
}

std::string TypeUtils::toString(const list_entry_t& val, void* valueVector) {
    auto listVector = (common::ValueVector*)valueVector;
    std::string result = "[";
    auto values = ListVector::getListValues(listVector, val);
    auto childType = VarListType::getChildType(&listVector->dataType);
    auto dataVector = ListVector::getDataVector(listVector);
    for (auto i = 0u; i < val.size; ++i) {
        result += castValueToString(*childType, values, dataVector);
        result += (val.size - 1 == i ? "]" : ",");
        values += ListVector::getDataVector(listVector)->getNumBytesPerValue();
    }
    return result;
}

std::string TypeUtils::toString(const struct_entry_t& val, void* valVector) {
    auto structVector = (common::ValueVector*)valVector;
    std::string result = "{";
    auto fields = StructType::getFields(&structVector->dataType);
    for (auto i = 0u; i < fields.size(); ++i) {
        auto field = fields[i];
        auto fieldVector = StructVector::getChildVector(structVector, i);
        auto value = fieldVector->getData() + fieldVector->getNumBytesPerValue() * val.pos;
        result += castValueToString(*field->getType(), value, fieldVector.get());
        result += (fields.size() - 1 == i ? "}" : ",");
    }
    return result;
}

std::string TypeUtils::prefixConversionExceptionMessage(
    const char* data, LogicalTypeID dataTypeID) {
    return "Cannot convert string " + std::string(data) + " to " +
           LogicalTypeUtils::dataTypeToString(dataTypeID) + ".";
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
    switch (VarListType::getChildType(&leftVector->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<uint8_t*>(leftValues)[i],
                    reinterpret_cast<uint8_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case LogicalTypeID::INT64: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<int64_t*>(leftValues)[i],
                    reinterpret_cast<int64_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case LogicalTypeID::INT32: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<int32_t*>(leftValues)[i],
                    reinterpret_cast<int32_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case LogicalTypeID::INT16: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<int16_t*>(leftValues)[i],
                    reinterpret_cast<int16_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case LogicalTypeID::DOUBLE: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<double_t*>(leftValues)[i],
                    reinterpret_cast<double_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case LogicalTypeID::FLOAT: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<float*>(leftValues)[i],
                    reinterpret_cast<float*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case LogicalTypeID::STRING: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<ku_string_t*>(leftValues)[i],
                    reinterpret_cast<ku_string_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case LogicalTypeID::DATE: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<date_t*>(leftValues)[i],
                    reinterpret_cast<date_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case LogicalTypeID::TIMESTAMP: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<timestamp_t*>(leftValues)[i],
                    reinterpret_cast<timestamp_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case LogicalTypeID::INTERVAL: {
        for (auto i = 0u; i < leftEntry.size; i++) {
            if (!isValueEqual(reinterpret_cast<interval_t*>(leftValues)[i],
                    reinterpret_cast<interval_t*>(rightValues)[i], left, right)) {
                return false;
            }
        }
    } break;
    case LogicalTypeID::VAR_LIST: {
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
                               LogicalTypeUtils::dataTypeToString(leftVector->dataType) +
                               " for TypeUtils::isValueEqual.");
    }
    }
    return true;
}

} // namespace common
} // namespace kuzu
