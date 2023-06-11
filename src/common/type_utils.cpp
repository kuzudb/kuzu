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
        auto fieldVector = StructVector::getFieldVector(structVector, i);
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
    auto leftDataVector = ListVector::getDataVector(leftVector);
    auto rightDataVector = ListVector::getDataVector(rightVector);
    for (auto i = 0u; i < leftEntry.size; i++) {
        auto leftPos = leftEntry.offset + i;
        auto rightPos = rightEntry.offset + i;
        if (leftDataVector->isNull(leftPos) && rightDataVector->isNull(rightPos)) {
            continue;
        } else if (leftDataVector->isNull(leftPos) != rightDataVector->isNull(rightPos)) {
            return false;
        }
        switch (leftDataVector->dataType.getPhysicalType()) {
        case PhysicalTypeID::BOOL: {
            if (!isValueEqual(leftDataVector->getValue<uint8_t>(leftPos),
                    rightDataVector->getValue<uint8_t>(rightPos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::INT64: {
            if (!isValueEqual(leftDataVector->getValue<int64_t>(leftPos),
                    rightDataVector->getValue<int64_t>(rightPos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::INT32: {
            if (!isValueEqual(leftDataVector->getValue<int32_t>(leftPos),
                    rightDataVector->getValue<int32_t>(rightPos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::INT16: {
            if (!isValueEqual(leftDataVector->getValue<int16_t>(leftPos),
                    rightDataVector->getValue<int16_t>(rightPos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::DOUBLE: {
            if (!isValueEqual(leftDataVector->getValue<double_t>(leftPos),
                    rightDataVector->getValue<double_t>(rightPos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::FLOAT: {
            if (!isValueEqual(leftDataVector->getValue<float_t>(leftPos),
                    rightDataVector->getValue<float_t>(rightPos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::STRING: {
            if (!isValueEqual(leftDataVector->getValue<ku_string_t>(leftPos),
                    rightDataVector->getValue<ku_string_t>(rightPos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::INTERVAL: {
            if (!isValueEqual(leftDataVector->getValue<interval_t>(leftPos),
                    rightDataVector->getValue<interval_t>(rightPos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::INTERNAL_ID: {
            if (!isValueEqual(leftDataVector->getValue<internalID_t>(leftPos),
                    rightDataVector->getValue<internalID_t>(rightPos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::VAR_LIST: {
            if (!isValueEqual(leftDataVector->getValue<list_entry_t>(leftPos),
                    rightDataVector->getValue<list_entry_t>(rightPos), leftDataVector,
                    rightDataVector)) {
                return false;
            }
        } break;
        case PhysicalTypeID::STRUCT: {
            if (!isValueEqual(leftDataVector->getValue<struct_entry_t>(leftPos),
                    rightDataVector->getValue<struct_entry_t>(rightPos), leftDataVector,
                    rightDataVector)) {
                return false;
            }
        } break;
        default: {
            throw NotImplementedException("TypeUtils::isValueEqual");
        }
        }
    }
    return true;
}

template<>
bool TypeUtils::isValueEqual(common::struct_entry_t& leftEntry, common::struct_entry_t& rightEntry,
    void* left, void* right) {
    auto leftVector = (ValueVector*)left;
    auto rightVector = (ValueVector*)right;
    if (leftVector->dataType != rightVector->dataType) {
        return false;
    }
    auto leftStructFields = common::StructVector::getFieldVectors(leftVector);
    auto rightStructFields = common::StructVector::getFieldVectors(rightVector);
    for (auto i = 0u; i < leftStructFields.size(); i++) {
        auto leftStructField = leftStructFields[i];
        auto rightStructField = rightStructFields[i];
        if (leftStructField->isNull(leftEntry.pos) && rightStructField->isNull(rightEntry.pos)) {
            continue;
        } else if (leftStructField->isNull(leftEntry.pos) !=
                   rightStructField->isNull(rightEntry.pos)) {
            return false;
        }
        switch (leftStructFields[i]->dataType.getPhysicalType()) {
        case PhysicalTypeID::BOOL: {
            if (!isValueEqual(leftStructField->getValue<uint8_t>(leftEntry.pos),
                    rightStructField->getValue<uint8_t>(rightEntry.pos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::INT64: {
            if (!isValueEqual(leftStructField->getValue<int64_t>(leftEntry.pos),
                    rightStructField->getValue<int64_t>(rightEntry.pos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::INT32: {
            if (!isValueEqual(leftStructField->getValue<int32_t>(leftEntry.pos),
                    rightStructField->getValue<int32_t>(rightEntry.pos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::INT16: {
            if (!isValueEqual(leftStructField->getValue<int16_t>(leftEntry.pos),
                    rightStructField->getValue<int16_t>(rightEntry.pos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::DOUBLE: {
            if (!isValueEqual(leftStructField->getValue<double_t>(leftEntry.pos),
                    rightStructField->getValue<double_t>(rightEntry.pos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::FLOAT: {
            if (!isValueEqual(leftStructField->getValue<float_t>(leftEntry.pos),
                    rightStructField->getValue<float_t>(rightEntry.pos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::STRING: {
            if (!isValueEqual(leftStructField->getValue<ku_string_t>(leftEntry.pos),
                    rightStructField->getValue<ku_string_t>(rightEntry.pos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::INTERVAL: {
            if (!isValueEqual(leftStructField->getValue<interval_t>(leftEntry.pos),
                    rightStructField->getValue<interval_t>(rightEntry.pos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::INTERNAL_ID: {
            if (!isValueEqual(leftStructField->getValue<internalID_t>(leftEntry.pos),
                    rightStructField->getValue<internalID_t>(rightEntry.pos), nullptr /* left */,
                    nullptr /* right */)) {
                return false;
            }
        } break;
        case PhysicalTypeID::VAR_LIST: {
            if (!isValueEqual(leftStructField->getValue<list_entry_t>(leftEntry.pos),
                    rightStructField->getValue<list_entry_t>(rightEntry.pos), leftStructField.get(),
                    rightStructField.get())) {
                return false;
            }
        } break;
        case PhysicalTypeID::STRUCT: {
            if (!isValueEqual(leftStructField->getValue<struct_entry_t>(leftEntry.pos),
                    rightStructField->getValue<struct_entry_t>(rightEntry.pos),
                    leftStructField.get(), rightStructField.get())) {
                return false;
            }
        } break;
        default: {
            throw NotImplementedException("TypeUtils::isValueEqual");
        }
        }
    }
    return true;
}

} // namespace common
} // namespace kuzu
