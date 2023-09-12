#include "common/types/value/value.h"

#include "common/null_buffer.h"
#include "common/ser_deser.h"
#include "common/types/blob.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace common {

void Value::setDataType(const LogicalType& dataType_) {
    assert(dataType->getLogicalTypeID() == LogicalTypeID::ANY);
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
        for (auto i = 0u; i < FixedListType::getNumElementsInList(&dataType); ++i) {
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
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL: {
        std::vector<std::unique_ptr<Value>> children;
        for (auto& field : StructType::getFields(&dataType)) {
            children.push_back(std::make_unique<Value>(createDefaultValue(*field->getType())));
        }
        return Value(dataType, std::move(children));
    }
    default:
        throw RuntimeException("Data type " + LogicalTypeUtils::dataTypeToString(dataType) +
                               " is not supported for Value::createDefaultValue");
    }
}

Value::Value(bool val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::BOOL);
    val.booleanVal = val_;
}

Value::Value(int8_t val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::INT8);
    val.int8Val = val_;
}

Value::Value(int16_t val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::INT16);
    val.int16Val = val_;
}

Value::Value(int32_t val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::INT32);
    val.int32Val = val_;
}

Value::Value(int64_t val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::INT64);
    val.int64Val = val_;
}

Value::Value(float_t val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::FLOAT);
    val.floatVal = val_;
}

Value::Value(double val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::DOUBLE);
    val.doubleVal = val_;
}

Value::Value(date_t val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::DATE);
    val.int32Val = val_.days;
}

Value::Value(timestamp_t val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP);
    val.int64Val = val_.value;
}

Value::Value(interval_t val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::INTERVAL);
    val.intervalVal = val_;
}

Value::Value(internalID_t val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID);
    val.internalIDVal = val_;
}

Value::Value(const char* val_) : isNull_{false} {
    dataType = std::make_unique<LogicalType>(LogicalTypeID::STRING);
    strVal = std::string(val_);
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

Value::Value(LogicalType dataType_, const uint8_t* val_) : isNull_{false} {
    dataType = dataType_.copy();
    copyValueFrom(val_);
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
    case LogicalTypeID::STRUCT: {
        copyFromStruct(value);
    } break;
    default:
        throw RuntimeException("Data type " + LogicalTypeUtils::dataTypeToString(*dataType) +
                               " is not supported for Value::set");
    }
}

void Value::copyValueFrom(const Value& other) {
    if (other.isNull()) {
        isNull_ = true;
        return;
    }
    isNull_ = false;
    assert(*dataType == *other.dataType);
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
    default:
        throw NotImplementedException("Value::Value(const Value&) for type " +
                                      LogicalTypeUtils::dataTypeToString(*dataType) +
                                      " is not implemented.");
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
    case LogicalTypeID::MAP: {
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
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST: {
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
    case LogicalTypeID::UNION: {
        // Only one member in the union can be active at a time and that member is always stored
        // at index 0.
        return children[0]->toString();
    }
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::STRUCT: {
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
    case LogicalTypeID::NODE: {
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
    case LogicalTypeID::REL: {
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
    default:
        throw NotImplementedException("Value::toString for type " +
                                      LogicalTypeUtils::dataTypeToString(*dataType) +
                                      " is not implemented.");
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

void Value::serialize(FileInfo* fileInfo, uint64_t& offset) const {
    dataType->serialize(fileInfo, offset);
    SerDeser::serializeValue(isNull_, fileInfo, offset);
    switch (dataType->getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        SerDeser::serializeValue(val.booleanVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::INT64: {
        SerDeser::serializeValue(val.int64Val, fileInfo, offset);
    } break;
    case PhysicalTypeID::INT32: {
        SerDeser::serializeValue(val.int32Val, fileInfo, offset);
    } break;
    case PhysicalTypeID::INT16: {
        SerDeser::serializeValue(val.int16Val, fileInfo, offset);
    } break;
    case PhysicalTypeID::INT8: {
        SerDeser::serializeValue(val.int8Val, fileInfo, offset);
    } break;
    case PhysicalTypeID::DOUBLE: {
        SerDeser::serializeValue(val.doubleVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::FLOAT: {
        SerDeser::serializeValue(val.floatVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::INTERVAL: {
        SerDeser::serializeValue(val.intervalVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        SerDeser::serializeValue(val.internalIDVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::STRING: {
        SerDeser::serializeValue(strVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::VAR_LIST:
    case PhysicalTypeID::FIXED_LIST:
    case PhysicalTypeID::STRUCT: {
        for (auto i = 0u; i < childrenSize; ++i) {
            children[i]->serialize(fileInfo, offset);
        }
    } break;
    default: {
        throw NotImplementedException{"Value::serialize"};
    }
    }
    SerDeser::serializeValue(childrenSize, fileInfo, offset);
}

std::unique_ptr<Value> Value::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    LogicalType dataType = *LogicalType::deserialize(fileInfo, offset);
    bool isNull;
    SerDeser::deserializeValue(isNull, fileInfo, offset);
    std::unique_ptr<Value> val = std::make_unique<Value>(createDefaultValue(dataType));
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        SerDeser::deserializeValue(val->val.booleanVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::INT64: {
        SerDeser::deserializeValue(val->val.int64Val, fileInfo, offset);
    } break;
    case PhysicalTypeID::INT32: {
        SerDeser::deserializeValue(val->val.int32Val, fileInfo, offset);
    } break;
    case PhysicalTypeID::INT16: {
        SerDeser::deserializeValue(val->val.int16Val, fileInfo, offset);
    } break;
    case PhysicalTypeID::INT8: {
        SerDeser::deserializeValue(val->val.int8Val, fileInfo, offset);
    } break;
    case PhysicalTypeID::DOUBLE: {
        SerDeser::deserializeValue(val->val.doubleVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::FLOAT: {
        SerDeser::deserializeValue(val->val.floatVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::INTERVAL: {
        SerDeser::deserializeValue(val->val.intervalVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        SerDeser::deserializeValue(val->val.internalIDVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::STRING: {
        SerDeser::deserializeValue(val->strVal, fileInfo, offset);
    } break;
    case PhysicalTypeID::VAR_LIST:
    case PhysicalTypeID::FIXED_LIST:
    case PhysicalTypeID::STRUCT: {
        SerDeser::deserializeVectorOfPtrs(val->children, fileInfo, offset);
    } break;
    default: {
        throw NotImplementedException{"Value::deserializeValue"};
    }
    }
    SerDeser::deserializeValue(val->childrenSize, fileInfo, offset);
    val->setNull(isNull);
    return val;
}

} // namespace common
} // namespace kuzu
