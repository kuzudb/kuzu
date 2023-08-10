#include "common/type_utils.h"

#include "common/exception.h"
#include "common/string_utils.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {

bool StringCastUtils::tryCastToBoolean(const char* data, uint64_t length, bool& result) {
    auto booleanStr = std::string{data, length};
    booleanStr = StringUtils::rtrim(StringUtils::ltrim(booleanStr));
    std::istringstream iss{booleanStr};
    iss >> std::boolalpha >> result;
    if (iss.fail()) {
        return false;
    }
    return true;
}

bool StringCastUtils::castToBool(const char* data, uint64_t length) {
    bool result;
    if (!tryCastToBoolean(data, length, result)) {
        throw ConversionException(
            TypeUtils::prefixConversionExceptionMessage(data, LogicalTypeID::BOOL) +
            ". Input is not equal to True or False (in a case-insensitive manner)");
    }
    return result;
}

void StringCastUtils::removeSpace(std::string& str) {
    str = StringUtils::rtrim(StringUtils::ltrim(str));
}

uint32_t TypeUtils::convertToUint32(const char* data) {
    std::istringstream iss(data);
    uint32_t val;
    if (!(iss >> val)) {
        throw ConversionException(
            StringUtils::string_format("Failed to convert {} to uint32_t", data));
    }
    return val;
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
    auto listVector = (ValueVector*)valueVector;
    if (val.size == 0) {
        return "[]";
    }
    std::string result = "[";
    auto values = ListVector::getListValues(listVector, val);
    auto childType = VarListType::getChildType(&listVector->dataType);
    auto dataVector = ListVector::getDataVector(listVector);
    for (auto i = 0u; i < val.size - 1; ++i) {
        result += castValueToString(*childType, values, dataVector);
        result += ",";
        values += ListVector::getDataVector(listVector)->getNumBytesPerValue();
    }
    result += castValueToString(*childType, values, dataVector);
    result += "]";
    return result;
}

std::string TypeUtils::toString(const struct_entry_t& val, void* valVector) {
    auto structVector = (ValueVector*)valVector;
    auto fields = StructType::getFields(&structVector->dataType);
    if (fields.size() == 0) {
        return "{}";
    }
    std::string result = "{";
    auto i = 0u;
    for (; i < fields.size() - 1; ++i) {
        auto fieldVector = StructVector::getFieldVector(structVector, i);
        result += castValueToString(*fields[i]->getType(),
            fieldVector->getData() + fieldVector->getNumBytesPerValue() * val.pos,
            fieldVector.get());
        result += ",";
    }
    auto fieldVector = StructVector::getFieldVector(structVector, i);
    result += castValueToString(*fields[i]->getType(),
        fieldVector->getData() + fieldVector->getNumBytesPerValue() * val.pos, fieldVector.get());
    result += "}";
    return result;
}

std::string TypeUtils::prefixConversionExceptionMessage(
    const char* data, LogicalTypeID dataTypeID) {
    return "Cannot convert string " + std::string(data) + " to " +
           LogicalTypeUtils::dataTypeToString(dataTypeID) + ".";
}

} // namespace common
} // namespace kuzu
