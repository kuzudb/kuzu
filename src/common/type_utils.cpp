#include "common/type_utils.h"

#include "common/types/blob.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {

std::string TypeUtils::castValueToString(
    const LogicalType& dataType, const uint8_t* value, void* vector) {
    auto valueVector = reinterpret_cast<ValueVector*>(vector);
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return TypeUtils::toString(*reinterpret_cast<const bool*>(value));
    case LogicalTypeID::INT64:
        return TypeUtils::toString(*reinterpret_cast<const int64_t*>(value));
    case LogicalTypeID::INT32:
        return TypeUtils::toString(*reinterpret_cast<const int32_t*>(value));
    case LogicalTypeID::INT16:
        return TypeUtils::toString(*reinterpret_cast<const int16_t*>(value));
    case LogicalTypeID::INT8:
        return TypeUtils::toString(*reinterpret_cast<const int8_t*>(value));
    case LogicalTypeID::UINT64:
        return TypeUtils::toString(*reinterpret_cast<const uint64_t*>(value));
    case LogicalTypeID::UINT32:
        return TypeUtils::toString(*reinterpret_cast<const uint32_t*>(value));
    case LogicalTypeID::UINT16:
        return TypeUtils::toString(*reinterpret_cast<const uint16_t*>(value));
    case LogicalTypeID::UINT8:
        return TypeUtils::toString(*reinterpret_cast<const uint8_t*>(value));
    case LogicalTypeID::INT128:
        return TypeUtils::toString(*reinterpret_cast<const int128_t*>(value));
    case LogicalTypeID::DOUBLE:
        return TypeUtils::toString(*reinterpret_cast<const double_t*>(value));
    case LogicalTypeID::FLOAT:
        return TypeUtils::toString(*reinterpret_cast<const float_t*>(value));
    case LogicalTypeID::DATE:
        return TypeUtils::toString(*reinterpret_cast<const date_t*>(value));
    case LogicalTypeID::TIMESTAMP:
        return TypeUtils::toString(*reinterpret_cast<const timestamp_t*>(value));
    case LogicalTypeID::INTERVAL:
        return TypeUtils::toString(*reinterpret_cast<const interval_t*>(value));
    case LogicalTypeID::STRING:
        return TypeUtils::toString(*reinterpret_cast<const ku_string_t*>(value));
    case LogicalTypeID::INTERNAL_ID:
        return TypeUtils::toString(*reinterpret_cast<const internalID_t*>(value));
    case LogicalTypeID::VAR_LIST:
        return TypeUtils::toString(*reinterpret_cast<const list_entry_t*>(value), valueVector);
    case LogicalTypeID::STRUCT:
        return TypeUtils::toString(*reinterpret_cast<const struct_entry_t*>(value), valueVector);
    default:
        KU_UNREACHABLE;
    }
}

template<>
std::string TypeUtils::toString(const int128_t& val, void* /*valueVector*/) {
    return Int128_t::ToString(val);
}

template<>
std::string TypeUtils::toString(const bool& val, void* /*valueVector*/) {
    return val ? "True" : "False";
}

template<>
std::string TypeUtils::toString(const internalID_t& val, void* /*valueVector*/) {
    return std::to_string(val.tableID) + ":" + std::to_string(val.offset);
}

template<>
std::string TypeUtils::toString(const date_t& val, void* /*valueVector*/) {
    return Date::toString(val);
}

template<>
std::string TypeUtils::toString(const timestamp_t& val, void* /*valueVector*/) {
    return Timestamp::toString(val);
}

template<>
std::string TypeUtils::toString(const interval_t& val, void* /*valueVector*/) {
    return Interval::toString(val);
}

template<>
std::string TypeUtils::toString(const ku_string_t& val, void* /*valueVector*/) {
    return val.getAsString();
}

template<>
std::string TypeUtils::toString(const blob_t& val, void* /*valueVector*/) {
    return Blob::toString(val.value.getData(), val.value.len);
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
std::string TypeUtils::toString(const map_entry_t& val, void* valueVector) {
    auto mapVector = (ValueVector*)valueVector;
    if (val.entry.size == 0) {
        return "{}";
    }
    std::string result = "{";
    auto keyType = MapType::getKeyType(&mapVector->dataType);
    auto valType = MapType::getValueType(&mapVector->dataType);
    auto dataVector = ListVector::getDataVector(mapVector);
    auto keyVector = MapVector::getKeyVector(mapVector);
    auto valVector = MapVector::getValueVector(mapVector);
    auto keyValues = keyVector->getData() + keyVector->getNumBytesPerValue() * val.entry.offset;
    auto valValues = valVector->getData() + valVector->getNumBytesPerValue() * val.entry.offset;
    for (auto i = 0u; i < val.entry.size - 1; ++i) {
        result += dataVector->isNull(val.entry.offset + i) ?
                      "" :
                      castValueToString(*keyType, keyValues, dataVector) + "=" +
                          castValueToString(*valType, valValues, dataVector);
        result += ", ";
        keyValues += keyVector->getNumBytesPerValue();
        valValues += valVector->getNumBytesPerValue();
    }
    result += dataVector->isNull(val.entry.offset + val.entry.size - 1) ?
                  "" :
                  castValueToString(*keyType, keyValues, dataVector) + "=" +
                      castValueToString(*valType, valValues, dataVector);
    result += "}";
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
        result += StructType::getField(&structVector->dataType, i)->getName();
        result += ": ";
        result += castValueToString(*fields[i]->getType(),
            fieldVector->getData() + fieldVector->getNumBytesPerValue() * val.pos,
            fieldVector.get());
        result += ", ";
    }
    auto fieldVector = StructVector::getFieldVector(structVector, i);
    result += StructType::getField(&structVector->dataType, i)->getName();
    result += ": ";
    result += castValueToString(*fields[i]->getType(),
        fieldVector->getData() + fieldVector->getNumBytesPerValue() * val.pos, fieldVector.get());
    result += "}";
    return result;
}

template<>
std::string TypeUtils::toString(const union_entry_t& val, void* valVector) {
    auto structVector = (ValueVector*)valVector;
    auto unionFieldIdx =
        UnionVector::getTagVector(structVector)->getValue<union_field_idx_t>(val.entry.pos);
    auto unionFieldVector = UnionVector::getValVector(structVector, unionFieldIdx);
    return castValueToString(unionFieldVector->dataType,
        unionFieldVector->getData() + unionFieldVector->getNumBytesPerValue() * val.entry.pos,
        unionFieldVector);
}

} // namespace common
} // namespace kuzu
