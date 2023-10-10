#include "common/type_utils.h"

#include "common/exception/conversion.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {

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

template<>
std::string TypeUtils::toString(const bool& val, void* valueVector) {
    return val ? "True" : "False";
}

template<>
std::string TypeUtils::toString(const internalID_t& val, void* valueVector) {
    return std::to_string(val.tableID) + ":" + std::to_string(val.offset);
}

template<>
std::string TypeUtils::toString(const date_t& val, void* valueVector) {
    return Date::toString(val);
}

template<>
std::string TypeUtils::toString(const timestamp_t& val, void* valueVector) {
    return Timestamp::toString(val);
}

template<>
std::string TypeUtils::toString(const interval_t& val, void* valueVector) {
    return Interval::toString(val);
}

template<>
std::string TypeUtils::toString(const ku_string_t& val, void* valueVector) {
    return val.getAsString();
}

template<>
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
        result += dataVector->isNull(val.offset + i) ?
                      "" :
                      castValueToString(*childType, values, dataVector);
        result += ",";
        values += ListVector::getDataVector(listVector)->getNumBytesPerValue();
    }
    result += dataVector->isNull(val.offset + val.size - 1) ?
                  "" :
                  castValueToString(*childType, values, dataVector);
    result += "]";
    return result;
}

template<>
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
