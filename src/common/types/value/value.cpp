#include "common/types/value/value.h"

#include "common/null_buffer.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/type_utils.h"
#include "common/types/blob.h"
#include "common/types/ku_string.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace common {

void Value::setDataType(const LogicalType& dataType_) {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::ANY);
    dataType = dataType_.copy();
}

LogicalType* Value::getDataType() const {
    return dataType.get();
}

void Value::setNull(bool flag) {
    isNull_ = flag;
}

void Value::setNull() {
    isNull_ = true;
}

bool Value::isNull() const {
    return isNull_;
}

std::unique_ptr<Value> Value::copy() const {
    return std::make_unique<Value>(*this);
}

Value Value::createNullValue() {
    return {};
}

Value Value::createNullValue(LogicalType dataType) {
    return Value(std::move(dataType));
}

Value Value::createDefaultValue(const LogicalType& dataType) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64:
        return Value((int64_t)0);
    case LogicalTypeID::INT32:
        return Value((int32_t)0);
    case LogicalTypeID::INT16:
        return Value((int16_t)0);
    case LogicalTypeID::INT8:
        return Value((int8_t)0);
    case LogicalTypeID::UINT64:
        return Value((uint64_t)0);
    case LogicalTypeID::UINT32:
        return Value((uint32_t)0);
    case LogicalTypeID::UINT16:
        return Value((uint16_t)0);
    case LogicalTypeID::UINT8:
        return Value((uint8_t)0);
    case LogicalTypeID::INT128:
        return Value(int128_t(0));
    case LogicalTypeID::BOOL:
        return Value(true);
    case LogicalTypeID::DOUBLE:
        return Value((double_t)0);
    case LogicalTypeID::DATE:
        return Value(date_t());
    case LogicalTypeID::TIMESTAMP:
        return Value(timestamp_t());
    case LogicalTypeID::INTERVAL:
        return Value(interval_t());
    case LogicalTypeID::INTERNAL_ID:
        return Value(nodeID_t());
    case LogicalTypeID::BLOB:
        return Value(LogicalType{LogicalTypeID::BLOB}, std::string(""));
    case LogicalTypeID::STRING:
        return Value(LogicalType{LogicalTypeID::STRING}, std::string(""));
    case LogicalTypeID::FLOAT:
        return Value((float_t)0);
    case LogicalTypeID::FIXED_LIST: {
        std::vector<std::unique_ptr<Value>> children;
        auto childType = FixedListType::getChildType(&dataType);
        for (auto i = 0u; i < FixedListType::getNumValuesInList(&dataType); ++i) {
            children.push_back(std::make_unique<Value>(createDefaultValue(*childType)));
        }
        return Value(dataType, std::move(children));
    }
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST: {
        return Value(dataType, std::vector<std::unique_ptr<Value>>{});
    }
    case LogicalTypeID::UNION: {
        std::vector<std::unique_ptr<Value>> children;
        children.push_back(std::make_unique<Value>(createNullValue()));
        return Value(dataType, std::move(children));
    }
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::RDF_VARIANT: {
        std::vector<std::unique_ptr<Value>> children;
        for (auto& field : StructType::getFields(&dataType)) {
            children.push_back(std::make_unique<Value>(createDefaultValue(*field->getType())));
        }
        return Value(dataType, std::move(children));
    }
    default:
        KU_UNREACHABLE;
    }
}

Value::Value(bool val_) : isNull_{false} {
    dataType = LogicalType::BOOL();
    val.booleanVal = val_;
}

Value::Value(int8_t val_) : isNull_{false} {
    dataType = LogicalType::INT8();
    val.int8Val = val_;
}

Value::Value(int16_t val_) : isNull_{false} {
    dataType = LogicalType::INT16();
    val.int16Val = val_;
}

Value::Value(int32_t val_) : isNull_{false} {
    dataType = LogicalType::INT32();
    val.int32Val = val_;
}

Value::Value(int64_t val_) : isNull_{false} {
    dataType = LogicalType::INT64();
    val.int64Val = val_;
}

Value::Value(uint8_t val_) : isNull_{false} {
    dataType = LogicalType::UINT8();
    val.uint8Val = val_;
}

Value::Value(uint16_t val_) : isNull_{false} {
    dataType = LogicalType::UINT16();
    val.uint16Val = val_;
}

Value::Value(uint32_t val_) : isNull_{false} {
    dataType = LogicalType::UINT32();
    val.uint32Val = val_;
}

Value::Value(uint64_t val_) : isNull_{false} {
    dataType = LogicalType::UINT64();
    val.uint64Val = val_;
}

Value::Value(int128_t val_) : isNull_{false} {
    dataType = LogicalType::INT128();
    val.int128Val = val_;
}

Value::Value(float_t val_) : isNull_{false} {
    dataType = LogicalType::FLOAT();
    val.floatVal = val_;
}

Value::Value(double val_) : isNull_{false} {
    dataType = LogicalType::DOUBLE();
    val.doubleVal = val_;
}

Value::Value(date_t val_) : isNull_{false} {
    dataType = LogicalType::DATE();
    val.int32Val = val_.days;
}

Value::Value(timestamp_t val_) : isNull_{false} {
    dataType = LogicalType::TIMESTAMP();
    val.int64Val = val_.value;
}

Value::Value(interval_t val_) : isNull_{false} {
    dataType = LogicalType::INTERVAL();
    val.intervalVal = val_;
}

Value::Value(internalID_t val_) : isNull_{false} {
    dataType = LogicalType::INTERNAL_ID();
    val.internalIDVal = val_;
}

Value::Value(const char* val_) : isNull_{false} {
    dataType = LogicalType::STRING();
    strVal = std::string(val_);
}

Value::Value(uint8_t* val_) : isNull_{false} {
    dataType = LogicalType::POINTER();
    val.pointer = val_;
}

Value::Value(LogicalType type, const std::string& val_) : isNull_{false} {
    dataType = type.copy();
    strVal = val_;
}

Value::Value(LogicalType dataType_, std::vector<std::unique_ptr<Value>> children) : isNull_{false} {
    dataType = dataType_.copy();
    this->children = std::move(children);
    childrenSize = this->children.size();
}

Value::Value(const Value& other) : isNull_{other.isNull_} {
    dataType = other.dataType->copy();
    copyValueFrom(other);
    childrenSize = other.childrenSize;
}

void Value::copyValueFrom(const uint8_t* value) {
    switch (dataType->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INT64: {
        val.int64Val = *((int64_t*)value);
    } break;
    case LogicalTypeID::DATE:
    case LogicalTypeID::INT32: {
        val.int32Val = *((int32_t*)value);
    } break;
    case LogicalTypeID::INT16: {
        val.int16Val = *((int16_t*)value);
    } break;
    case LogicalTypeID::INT8: {
        val.int8Val = *((int8_t*)value);
    } break;
    case LogicalTypeID::UINT64: {
        val.uint64Val = *((uint64_t*)value);
    } break;
    case LogicalTypeID::UINT32: {
        val.uint32Val = *((uint32_t*)value);
    } break;
    case LogicalTypeID::UINT16: {
        val.uint16Val = *((uint16_t*)value);
    } break;
    case LogicalTypeID::UINT8: {
        val.uint8Val = *((uint8_t*)value);
    } break;
    case LogicalTypeID::INT128: {
        val.int128Val = *((int128_t*)value);
    } break;
    case LogicalTypeID::BOOL: {
        val.booleanVal = *((bool*)value);
    } break;
    case LogicalTypeID::DOUBLE: {
        val.doubleVal = *((double*)value);
    } break;
    case LogicalTypeID::FLOAT: {
        val.floatVal = *((float_t*)value);
    } break;
    case LogicalTypeID::INTERVAL: {
        val.intervalVal = *((interval_t*)value);
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        val.internalIDVal = *((nodeID_t*)value);
    } break;
    case LogicalTypeID::BLOB: {
        strVal = ((blob_t*)value)->value.getAsString();
    } break;
    case LogicalTypeID::STRING: {
        strVal = ((ku_string_t*)value)->getAsString();
    } break;
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST: {
        copyFromVarList(*(ku_list_t*)value, *VarListType::getChildType(dataType.get()));
    } break;
    case LogicalTypeID::FIXED_LIST: {
        copyFromFixedList(value);
    } break;
    case LogicalTypeID::UNION: {
        copyFromUnion(value);
    } break;
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::RDF_VARIANT: {
        copyFromStruct(value);
    } break;
    case LogicalTypeID::POINTER: {
        val.pointer = *((uint8_t**)value);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void Value::copyValueFrom(const Value& other) {
    if (other.isNull()) {
        isNull_ = true;
        return;
    }
    isNull_ = false;
    KU_ASSERT(*dataType == *other.dataType);
    switch (dataType->getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        val.booleanVal = other.val.booleanVal;
    } break;
    case PhysicalTypeID::INT64: {
        val.int64Val = other.val.int64Val;
    } break;
    case PhysicalTypeID::INT32: {
        val.int32Val = other.val.int32Val;
    } break;
    case PhysicalTypeID::INT16: {
        val.int16Val = other.val.int16Val;
    } break;
    case PhysicalTypeID::INT8: {
        val.int8Val = other.val.int8Val;
    } break;
    case PhysicalTypeID::UINT64: {
        val.uint64Val = other.val.uint64Val;
    } break;
    case PhysicalTypeID::UINT32: {
        val.uint32Val = other.val.uint32Val;
    } break;
    case PhysicalTypeID::UINT16: {
        val.uint16Val = other.val.uint16Val;
    } break;
    case PhysicalTypeID::UINT8: {
        val.uint8Val = other.val.uint8Val;
    } break;
    case PhysicalTypeID::INT128: {
        val.int128Val = other.val.int128Val;
    } break;
    case PhysicalTypeID::DOUBLE: {
        val.doubleVal = other.val.doubleVal;
    } break;
    case PhysicalTypeID::FLOAT: {
        val.floatVal = other.val.floatVal;
    } break;
    case PhysicalTypeID::INTERVAL: {
        val.intervalVal = other.val.intervalVal;
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        val.internalIDVal = other.val.internalIDVal;
    } break;
    case PhysicalTypeID::STRING: {
        strVal = other.strVal;
    } break;
    case PhysicalTypeID::VAR_LIST:
    case PhysicalTypeID::FIXED_LIST:
    case PhysicalTypeID::STRUCT: {
        for (auto& child : other.children) {
            children.push_back(child->copy());
        }
    } break;
    case PhysicalTypeID::POINTER: {
        val.pointer = other.val.pointer;
    } break;
    default:
        KU_UNREACHABLE;
    }
}

std::string Value::toString() const {
    if (isNull_) {
        return "";
    }
    switch (dataType->getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return TypeUtils::toString(val.booleanVal);
    case LogicalTypeID::INT64:
        return TypeUtils::toString(val.int64Val);
    case LogicalTypeID::INT32:
        return TypeUtils::toString(val.int32Val);
    case LogicalTypeID::INT16:
        return TypeUtils::toString(val.int16Val);
    case LogicalTypeID::INT8:
        return TypeUtils::toString(val.int8Val);
    case LogicalTypeID::UINT64:
        return TypeUtils::toString(val.uint64Val);
    case LogicalTypeID::UINT32:
        return TypeUtils::toString(val.uint32Val);
    case LogicalTypeID::UINT16:
        return TypeUtils::toString(val.uint16Val);
    case LogicalTypeID::UINT8:
        return TypeUtils::toString(val.uint8Val);
    case LogicalTypeID::INT128:
        return TypeUtils::toString(val.int128Val);
    case LogicalTypeID::DOUBLE:
        return TypeUtils::toString(val.doubleVal);
    case LogicalTypeID::FLOAT:
        return TypeUtils::toString(val.floatVal);
    case LogicalTypeID::DATE:
        return TypeUtils::toString(date_t{val.int32Val});
    case LogicalTypeID::TIMESTAMP:
        return TypeUtils::toString(timestamp_t{val.int64Val});
    case LogicalTypeID::INTERVAL:
        return TypeUtils::toString(val.intervalVal);
    case LogicalTypeID::INTERNAL_ID:
        return TypeUtils::toString(val.internalIDVal);
    case LogicalTypeID::BLOB:
        return Blob::toString(reinterpret_cast<const uint8_t*>(strVal.c_str()), strVal.length());
    case LogicalTypeID::STRING:
        return strVal;
    case LogicalTypeID::RDF_VARIANT: {
        return rdfVariantToString();
    }
    case LogicalTypeID::MAP: {
        return mapToString();
    }
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST: {
        return listToString();
    }
    case LogicalTypeID::UNION: {
        // Only one member in the union can be active at a time and that member is always stored
        // at index 0.
        return children[0]->toString();
    }
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::STRUCT: {
        return structToString();
    }
    case LogicalTypeID::NODE: {
        return nodeToString();
    }
    case LogicalTypeID::REL: {
        return relToString();
    }
    default:
        KU_UNREACHABLE;
    }
}

Value::Value() : isNull_{true} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::ANY);
}

Value::Value(LogicalType dataType_) : isNull_{true} {
    dataType = dataType_.copy();
}

void Value::copyFromFixedList(const uint8_t* fixedList) {
    auto numBytesPerElement =
        storage::StorageUtils::getDataTypeSize(*FixedListType::getChildType(dataType.get()));
    for (auto i = 0; i < childrenSize; ++i) {
        auto childValue = children[i].get();
        childValue->copyValueFrom(fixedList + i * numBytesPerElement);
    }
}

void Value::copyFromVarList(ku_list_t& list, const LogicalType& childType) {
    if (list.size > children.size()) {
        children.reserve(list.size);
        for (auto i = children.size(); i < list.size; ++i) {
            children.push_back(std::make_unique<Value>(createDefaultValue(childType)));
        }
    }
    childrenSize = list.size;
    auto numBytesPerElement = storage::StorageUtils::getDataTypeSize(childType);
    auto listNullBytes = reinterpret_cast<uint8_t*>(list.overflowPtr);
    auto numBytesForNullValues = NullBuffer::getNumBytesForNullValues(list.size);
    auto listValues = listNullBytes + numBytesForNullValues;
    for (auto i = 0; i < list.size; i++) {
        auto childValue = children[i].get();
        if (NullBuffer::isNull(listNullBytes, i)) {
            childValue->setNull(true);
        } else {
            childValue->setNull(false);
            childValue->copyValueFrom(listValues);
        }
        listValues += numBytesPerElement;
    }
}

void Value::copyFromStruct(const uint8_t* kuStruct) {
    auto numFields = childrenSize;
    auto structNullValues = kuStruct;
    auto structValues = structNullValues + NullBuffer::getNumBytesForNullValues(numFields);
    for (auto i = 0; i < numFields; i++) {
        auto childValue = children[i].get();
        if (NullBuffer::isNull(structNullValues, i)) {
            childValue->setNull(true);
        } else {
            childValue->setNull(false);
            childValue->copyValueFrom(structValues);
        }
        structValues += storage::StorageUtils::getDataTypeSize(*childValue->dataType);
    }
}

void Value::copyFromUnion(const uint8_t* kuUnion) {
    auto childrenTypes = StructType::getFieldTypes(dataType.get());
    auto unionNullValues = kuUnion;
    auto unionValues = unionNullValues + NullBuffer::getNumBytesForNullValues(childrenTypes.size());
    // For union dataType, only one member can be active at a time. So we don't need to copy all
    // union fields into value.
    auto activeFieldIdx = UnionType::getInternalFieldIdx(*(union_field_idx_t*)unionValues);
    auto childValue = children[0].get();
    childValue->dataType = childrenTypes[activeFieldIdx]->copy();
    auto curMemberIdx = 0u;
    // Seek to the current active member value.
    while (curMemberIdx < activeFieldIdx) {
        unionValues += storage::StorageUtils::getDataTypeSize(*childrenTypes[curMemberIdx]);
        curMemberIdx++;
    }
    if (NullBuffer::isNull(unionNullValues, activeFieldIdx)) {
        childValue->setNull(true);
    } else {
        childValue->setNull(false);
        childValue->copyValueFrom(unionValues);
    }
}

void Value::serialize(Serializer& serializer) const {
    dataType->serialize(serializer);
    serializer.serializeValue(isNull_);
    switch (dataType->getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        serializer.serializeValue(val.booleanVal);
    } break;
    case PhysicalTypeID::INT64: {
        serializer.serializeValue(val.int64Val);
    } break;
    case PhysicalTypeID::INT32: {
        serializer.serializeValue(val.int32Val);
    } break;
    case PhysicalTypeID::INT16: {
        serializer.serializeValue(val.int16Val);
    } break;
    case PhysicalTypeID::INT8: {
        serializer.serializeValue(val.int8Val);
    } break;
    case PhysicalTypeID::UINT64: {
        serializer.serializeValue(val.uint64Val);
    } break;
    case PhysicalTypeID::UINT32: {
        serializer.serializeValue(val.uint32Val);
    } break;
    case PhysicalTypeID::UINT16: {
        serializer.serializeValue(val.uint16Val);
    } break;
    case PhysicalTypeID::UINT8: {
        serializer.serializeValue(val.uint8Val);
    } break;
    case PhysicalTypeID::INT128: {
        serializer.serializeValue(val.int128Val);
    } break;
    case PhysicalTypeID::DOUBLE: {
        serializer.serializeValue(val.doubleVal);
    } break;
    case PhysicalTypeID::FLOAT: {
        serializer.serializeValue(val.floatVal);
    } break;
    case PhysicalTypeID::INTERVAL: {
        serializer.serializeValue(val.intervalVal);
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        serializer.serializeValue(val.internalIDVal);
    } break;
    case PhysicalTypeID::STRING: {
        serializer.serializeValue(strVal);
    } break;
    case PhysicalTypeID::VAR_LIST:
    case PhysicalTypeID::FIXED_LIST:
    case PhysicalTypeID::STRUCT: {
        for (auto i = 0u; i < childrenSize; ++i) {
            children[i]->serialize(serializer);
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    serializer.serializeValue(childrenSize);
}

std::unique_ptr<Value> Value::deserialize(Deserializer& deserializer) {
    LogicalType dataType = *LogicalType::deserialize(deserializer);
    bool isNull;
    deserializer.deserializeValue(isNull);
    std::unique_ptr<Value> val = std::make_unique<Value>(createDefaultValue(dataType));
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        deserializer.deserializeValue(val->val.booleanVal);
    } break;
    case PhysicalTypeID::INT64: {
        deserializer.deserializeValue(val->val.int64Val);
    } break;
    case PhysicalTypeID::INT32: {
        deserializer.deserializeValue(val->val.int32Val);
    } break;
    case PhysicalTypeID::INT16: {
        deserializer.deserializeValue(val->val.int16Val);
    } break;
    case PhysicalTypeID::INT8: {
        deserializer.deserializeValue(val->val.int8Val);
    } break;
    case PhysicalTypeID::UINT64: {
        deserializer.deserializeValue(val->val.uint64Val);
    } break;
    case PhysicalTypeID::UINT32: {
        deserializer.deserializeValue(val->val.uint32Val);
    } break;
    case PhysicalTypeID::UINT16: {
        deserializer.deserializeValue(val->val.uint16Val);
    } break;
    case PhysicalTypeID::UINT8: {
        deserializer.deserializeValue(val->val.uint8Val);
    } break;
    case PhysicalTypeID::INT128: {
        deserializer.deserializeValue(val->val.int128Val);
    } break;
    case PhysicalTypeID::DOUBLE: {
        deserializer.deserializeValue(val->val.doubleVal);
    } break;
    case PhysicalTypeID::FLOAT: {
        deserializer.deserializeValue(val->val.floatVal);
    } break;
    case PhysicalTypeID::INTERVAL: {
        deserializer.deserializeValue(val->val.intervalVal);
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        deserializer.deserializeValue(val->val.internalIDVal);
    } break;
    case PhysicalTypeID::STRING: {
        deserializer.deserializeValue(val->strVal);
    } break;
    case PhysicalTypeID::VAR_LIST:
    case PhysicalTypeID::FIXED_LIST:
    case PhysicalTypeID::STRUCT: {
        deserializer.deserializeVectorOfPtrs(val->children);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    deserializer.deserializeValue(val->childrenSize);
    val->setNull(isNull);
    return val;
}

std::string Value::rdfVariantToString() const {
    auto type = static_cast<LogicalTypeID>(children[0]->val.uint8Val);
    switch (type) {
    case LogicalTypeID::STRING:
        return children[1]->strVal;
    case LogicalTypeID::INT64: {
        return TypeUtils::toString(Blob::getValue<int64_t>(children[1]->strVal.data()));
    }
    case LogicalTypeID::DOUBLE: {
        return TypeUtils::toString(Blob::getValue<double_t>(children[1]->strVal.data()));
    }
    case LogicalTypeID::BOOL: {
        return TypeUtils::toString(Blob::getValue<bool>(children[1]->strVal.data()));
    }
    case LogicalTypeID::DATE: {
        return TypeUtils::toString(Blob::getValue<date_t>(children[1]->strVal.data()));
    }
    default:
        return children[1]->strVal;
    }
}

std::string Value::mapToString() const {
    std::string result = "{";
    for (auto i = 0u; i < childrenSize; ++i) {
        auto structVal = children[i].get();
        result += structVal->children[0]->toString();
        result += "=";
        result += structVal->children[1]->toString();
        result += (i == childrenSize - 1 ? "" : ", ");
    }
    result += "}";
    return result;
}

std::string Value::listToString() const {
    std::string result = "[";
    for (auto i = 0u; i < childrenSize; ++i) {
        result += children[i]->toString();
        if (i != childrenSize - 1) {
            result += ",";
        }
    }
    result += "]";
    return result;
}

std::string Value::structToString() const {
    std::string result = "{";
    auto fieldNames = StructType::getFieldNames(dataType.get());
    for (auto i = 0u; i < childrenSize; ++i) {
        result += fieldNames[i] + ": ";
        result += children[i]->toString();
        if (i != childrenSize - 1) {
            result += ", ";
        }
    }
    result += "}";
    return result;
}

std::string Value::nodeToString() const {
    if (children[0]->isNull_) {
        // NODE is represented as STRUCT. We don't have a way to represent STRUCT as null.
        // Instead, we check the internal ID entry to decide if a NODE is NULL.
        return "";
    }
    std::string result = "{";
    auto fieldNames = StructType::getFieldNames(dataType.get());
    for (auto i = 0u; i < childrenSize; ++i) {
        if (children[i]->isNull_) {
            // Avoid printing null key value pair.
            continue;
        }
        if (i != 0) {
            result += ", ";
        }
        result += fieldNames[i] + ": " + children[i]->toString();
    }
    result += "}";
    return result;
}

std::string Value::relToString() const {
    if (children[3]->isNull_) {
        // REL is represented as STRUCT. We don't have a way to represent STRUCT as null.
        // Instead, we check the internal ID entry to decide if a REL is NULL.
        return "";
    }
    std::string result = "(" + children[0]->toString() + ")-{";
    auto fieldNames = StructType::getFieldNames(dataType.get());
    for (auto i = 2u; i < childrenSize; ++i) {
        if (children[i]->isNull_) {
            // Avoid printing null key value pair.
            continue;
        }
        if (i != 2) {
            result += ", ";
        }
        result += fieldNames[i] + ": " + children[i]->toString();
    }
    result += "}->(" + children[1]->toString() + ")";
    return result;
}

} // namespace common
} // namespace kuzu
