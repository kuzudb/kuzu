#include "common/types/value.h"

#include "common/null_buffer.h"
#include "common/string_utils.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace common {

void Value::setDataType(const LogicalType& dataType_) {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::ANY);
    dataType = dataType_;
}

LogicalType Value::getDataType() const {
    return dataType;
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
    case LogicalTypeID::INT64:
        return Value((int64_t)0);
    case LogicalTypeID::INT32:
        return Value((int32_t)0);
    case LogicalTypeID::INT16:
        return Value((int16_t)0);
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
    case LogicalTypeID::STRING:
        return Value(std::string(""));
    case LogicalTypeID::FLOAT:
        return Value((float_t)0);
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST:
    case LogicalTypeID::STRUCT:
        return Value(dataType, std::vector<std::unique_ptr<Value>>{});
    default:
        throw RuntimeException("Data type " + LogicalTypeUtils::dataTypeToString(dataType) +
                               " is not supported for Value::createDefaultValue");
    }
}

Value::Value(bool val_) : dataType{LogicalTypeID::BOOL}, isNull_{false} {
    val.booleanVal = val_;
}

Value::Value(int16_t val_) : dataType{LogicalTypeID::INT16}, isNull_{false} {
    val.int16Val = val_;
}

Value::Value(int32_t val_) : dataType{LogicalTypeID::INT32}, isNull_{false} {
    val.int32Val = val_;
}

Value::Value(int64_t val_) : dataType{LogicalTypeID::INT64}, isNull_{false} {
    val.int64Val = val_;
}

Value::Value(float_t val_) : dataType{LogicalTypeID::FLOAT}, isNull_{false} {
    val.floatVal = val_;
}

Value::Value(double val_) : dataType{LogicalTypeID::DOUBLE}, isNull_{false} {
    val.doubleVal = val_;
}

Value::Value(date_t val_) : dataType{LogicalTypeID::DATE}, isNull_{false} {
    val.dateVal = val_;
}

Value::Value(kuzu::common::timestamp_t val_) : dataType{LogicalTypeID::TIMESTAMP}, isNull_{false} {
    val.timestampVal = val_;
}

Value::Value(kuzu::common::interval_t val_) : dataType{LogicalTypeID::INTERVAL}, isNull_{false} {
    val.intervalVal = val_;
}

Value::Value(kuzu::common::internalID_t val_)
    : dataType{LogicalTypeID::INTERNAL_ID}, isNull_{false} {
    val.internalIDVal = val_;
}

Value::Value(const char* val_) : dataType{LogicalTypeID::STRING}, isNull_{false} {
    strVal = std::string(val_);
}

Value::Value(const std::string& val_) : dataType{LogicalTypeID::STRING}, isNull_{false} {
    strVal = val_;
}

Value::Value(LogicalType dataType, std::vector<std::unique_ptr<Value>> vals)
    : dataType{std::move(dataType)}, isNull_{false} {
    nestedTypeVal = std::move(vals);
}

Value::Value(std::unique_ptr<NodeVal> val_) : dataType{LogicalTypeID::NODE}, isNull_{false} {
    nodeVal = std::move(val_);
}

Value::Value(std::unique_ptr<RelVal> val_) : dataType{LogicalTypeID::REL}, isNull_{false} {
    relVal = std::move(val_);
}

Value::Value(LogicalType dataType, const uint8_t* val_)
    : dataType{std::move(dataType)}, isNull_{false} {
    copyValueFrom(val_);
}

Value::Value(const Value& other) : dataType{other.dataType}, isNull_{other.isNull_} {
    copyValueFrom(other);
}

void Value::copyValueFrom(const uint8_t* value) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        val.int64Val = *((int64_t*)value);
    } break;
    case LogicalTypeID::INT32: {
        val.int32Val = *((int32_t*)value);
    } break;
    case LogicalTypeID::INT16: {
        val.int16Val = *((int16_t*)value);
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
    case LogicalTypeID::DATE: {
        val.dateVal = *((date_t*)value);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        val.timestampVal = *((timestamp_t*)value);
    } break;
    case LogicalTypeID::INTERVAL: {
        val.intervalVal = *((interval_t*)value);
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        val.internalIDVal = *((nodeID_t*)value);
    } break;
    case LogicalTypeID::STRING: {
        strVal = ((ku_string_t*)value)->getAsString();
    } break;
    case LogicalTypeID::VAR_LIST: {
        nestedTypeVal = convertKUVarListToVector(*(ku_list_t*)value);
    } break;
    case LogicalTypeID::FIXED_LIST: {
        nestedTypeVal = convertKUFixedListToVector(value);
    } break;
    case LogicalTypeID::STRUCT: {
        nestedTypeVal = convertKUStructToVector(value);
    } break;
    default:
        throw RuntimeException("Data type " + LogicalTypeUtils::dataTypeToString(dataType) +
                               " is not supported for Value::set");
    }
}

void Value::copyValueFrom(const Value& other) {
    if (other.isNull()) {
        isNull_ = true;
        return;
    }
    isNull_ = false;
    assert(dataType == other.dataType);
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        val.booleanVal = other.val.booleanVal;
    } break;
    case LogicalTypeID::INT64: {
        val.int64Val = other.val.int64Val;
    } break;
    case LogicalTypeID::INT32: {
        val.int32Val = other.val.int32Val;
    } break;
    case LogicalTypeID::INT16: {
        val.int16Val = other.val.int16Val;
    } break;
    case LogicalTypeID::DOUBLE: {
        val.doubleVal = other.val.doubleVal;
    } break;
    case LogicalTypeID::FLOAT: {
        val.floatVal = other.val.floatVal;
    } break;
    case LogicalTypeID::DATE: {
        val.dateVal = other.val.dateVal;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        val.timestampVal = other.val.timestampVal;
    } break;
    case LogicalTypeID::INTERVAL: {
        val.intervalVal = other.val.intervalVal;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        val.internalIDVal = other.val.internalIDVal;
    } break;
    case LogicalTypeID::STRING: {
        strVal = other.strVal;
    } break;
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST:
    case LogicalTypeID::STRUCT: {
        for (auto& value : other.nestedTypeVal) {
            nestedTypeVal.push_back(value->copy());
        }
    } break;
    case LogicalTypeID::NODE: {
        nodeVal = other.nodeVal->copy();
    } break;
    case LogicalTypeID::REL: {
        relVal = other.relVal->copy();
    } break;
    default:
        throw NotImplementedException("Value::Value(const Value&) for type " +
                                      LogicalTypeUtils::dataTypeToString(dataType) +
                                      " is not implemented.");
    }
}

const std::vector<std::unique_ptr<Value>>& Value::getListValReference() const {
    return nestedTypeVal;
}

std::string Value::toString() const {
    if (isNull_) {
        return "";
    }
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return TypeUtils::toString(val.booleanVal);
    case LogicalTypeID::INT64:
        return TypeUtils::toString(val.int64Val);
    case LogicalTypeID::INT32:
        return TypeUtils::toString(val.int32Val);
    case LogicalTypeID::INT16:
        return TypeUtils::toString(val.int16Val);
    case LogicalTypeID::DOUBLE:
        return TypeUtils::toString(val.doubleVal);
    case LogicalTypeID::FLOAT:
        return TypeUtils::toString(val.floatVal);
    case LogicalTypeID::DATE:
        return TypeUtils::toString(val.dateVal);
    case LogicalTypeID::TIMESTAMP:
        return TypeUtils::toString(val.timestampVal);
    case LogicalTypeID::INTERVAL:
        return TypeUtils::toString(val.intervalVal);
    case LogicalTypeID::INTERNAL_ID:
        return TypeUtils::toString(val.internalIDVal);
    case LogicalTypeID::STRING:
        return strVal;
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST: {
        std::string result = "[";
        for (auto i = 0u; i < nestedTypeVal.size(); ++i) {
            result += nestedTypeVal[i]->toString();
            if (i != nestedTypeVal.size() - 1) {
                result += ",";
            }
        }
        result += "]";
        return result;
    }
    case LogicalTypeID::STRUCT: {
        std::string result = "{";
        auto childrenNames = common::StructType::getStructFieldNames(&dataType);
        for (auto i = 0u; i < nestedTypeVal.size(); ++i) {
            result += childrenNames[i];
            result += ": ";
            result += nestedTypeVal[i]->toString();
            if (i != nestedTypeVal.size() - 1) {
                result += ", ";
            }
        }
        result += "}";
        return result;
    }
    case LogicalTypeID::NODE:
        return nodeVal->toString();
    case LogicalTypeID::REL:
        return relVal->toString();
    default:
        throw NotImplementedException("Value::toString for type " +
                                      LogicalTypeUtils::dataTypeToString(dataType) +
                                      " is not implemented.");
    }
}

Value::Value() : dataType{LogicalTypeID::ANY}, isNull_{true} {}

Value::Value(LogicalType dataType) : dataType{std::move(dataType)}, isNull_{true} {}

std::vector<std::unique_ptr<Value>> Value::convertKUVarListToVector(ku_list_t& list) const {
    std::vector<std::unique_ptr<Value>> listResultValue;
    auto childType = VarListType::getChildType(&dataType);
    auto numBytesPerElement = storage::StorageUtils::getDataTypeSize(*childType);
    auto listNullBytes = reinterpret_cast<uint8_t*>(list.overflowPtr);
    auto numBytesForNullValues = NullBuffer::getNumBytesForNullValues(list.size);
    auto listValues = listNullBytes + numBytesForNullValues;
    for (auto i = 0; i < list.size; i++) {
        auto childValue = std::make_unique<Value>(Value::createDefaultValue(*childType));
        if (NullBuffer::isNull(listNullBytes, i)) {
            childValue->setNull();
        } else {
            childValue->copyValueFrom(listValues);
        }
        listResultValue.push_back(std::move(childValue));
        listValues += numBytesPerElement;
    }
    return listResultValue;
}

std::vector<std::unique_ptr<Value>> Value::convertKUFixedListToVector(
    const uint8_t* fixedList) const {
    auto numElementsInList = FixedListType::getNumElementsInList(&dataType);
    std::vector<std::unique_ptr<Value>> fixedListResultVal{numElementsInList};
    auto childType = FixedListType::getChildType(&dataType);
    auto numBytesPerElement = storage::StorageUtils::getDataTypeSize(*childType);
    switch (childType->getLogicalTypeID()) {
    case common::LogicalTypeID::INT64: {
        putValuesIntoVector<int64_t>(fixedListResultVal, fixedList, numBytesPerElement);
    } break;
    case common::LogicalTypeID::INT32: {
        putValuesIntoVector<int32_t>(fixedListResultVal, fixedList, numBytesPerElement);
    } break;
    case common::LogicalTypeID::INT16: {
        putValuesIntoVector<int16_t>(fixedListResultVal, fixedList, numBytesPerElement);
    } break;
    case common::LogicalTypeID::DOUBLE: {
        putValuesIntoVector<double_t>(fixedListResultVal, fixedList, numBytesPerElement);
    } break;
    case common::LogicalTypeID::FLOAT: {
        putValuesIntoVector<float_t>(fixedListResultVal, fixedList, numBytesPerElement);
    } break;
    default:
        assert(false);
    }
    return fixedListResultVal;
}

std::vector<std::unique_ptr<Value>> Value::convertKUStructToVector(const uint8_t* kuStruct) const {
    std::vector<std::unique_ptr<Value>> structVal;
    auto childrenTypes = StructType::getStructFieldTypes(&dataType);
    auto numFields = childrenTypes.size();
    auto structNullValues = kuStruct;
    auto structValues = structNullValues + NullBuffer::getNumBytesForNullValues(numFields);
    for (auto i = 0; i < numFields; i++) {
        auto childValue = std::make_unique<Value>(Value::createDefaultValue(*childrenTypes[i]));
        if (NullBuffer::isNull(structNullValues, i)) {
            childValue->setNull(true);
        } else {
            childValue->copyValueFrom(structValues);
        }
        structVal.emplace_back(std::move(childValue));
        structValues += storage::StorageUtils::getDataTypeSize(*childrenTypes[i]);
    }
    return structVal;
}

static std::string propertiesToString(
    const std::vector<std::pair<std::string, std::unique_ptr<Value>>>& properties) {
    std::string result = "{";
    for (auto i = 0u; i < properties.size(); ++i) {
        auto& [name, value] = properties[i];
        result += name + ":" + value->toString();
        result += (i == properties.size() - 1 ? "" : ", ");
    }
    result += "}";
    return result;
}

NodeVal::NodeVal(std::unique_ptr<Value> idVal, std::unique_ptr<Value> labelVal)
    : idVal{std::move(idVal)}, labelVal{std::move(labelVal)} {}

NodeVal::NodeVal(const NodeVal& other) {
    idVal = other.idVal->copy();
    labelVal = other.labelVal->copy();
    for (auto& [key, val] : other.properties) {
        addProperty(key, val->copy());
    }
}

void NodeVal::addProperty(const std::string& key, std::unique_ptr<Value> value) {
    properties.emplace_back(key, std::move(value));
}

const std::vector<std::pair<std::string, std::unique_ptr<Value>>>& NodeVal::getProperties() const {
    return properties;
}

Value* NodeVal::getNodeIDVal() {
    return idVal.get();
}

Value* NodeVal::getLabelVal() {
    return labelVal.get();
}

nodeID_t NodeVal::getNodeID() const {
    return idVal->getValue<nodeID_t>();
}

std::string NodeVal::getLabelName() const {
    return labelVal->getValue<std::string>();
}

std::unique_ptr<NodeVal> NodeVal::copy() const {
    return std::make_unique<NodeVal>(*this);
}

std::string NodeVal::toString() const {
    std::string result = "(";
    result += "label:" + labelVal->toString() + ", ";
    result += idVal->toString() + ", ";
    result += propertiesToString(properties);
    result += ")";
    return result;
}

RelVal::RelVal(std::unique_ptr<Value> srcNodeIDVal, std::unique_ptr<Value> dstNodeIDVal,
    std::unique_ptr<Value> labelVal)
    : srcNodeIDVal{std::move(srcNodeIDVal)},
      dstNodeIDVal{std::move(dstNodeIDVal)}, labelVal{std::move(labelVal)} {}

RelVal::RelVal(const RelVal& other) {
    srcNodeIDVal = other.srcNodeIDVal->copy();
    dstNodeIDVal = other.dstNodeIDVal->copy();
    labelVal = other.labelVal->copy();
    for (auto& [key, val] : other.properties) {
        addProperty(key, val->copy());
    }
}

void RelVal::addProperty(const std::string& key, std::unique_ptr<Value> value) {
    properties.emplace_back(key, std::move(value));
}

const std::vector<std::pair<std::string, std::unique_ptr<Value>>>& RelVal::getProperties() const {
    return properties;
}

Value* RelVal::getSrcNodeIDVal() {
    return srcNodeIDVal.get();
}

Value* RelVal::getDstNodeIDVal() {
    return dstNodeIDVal.get();
}

nodeID_t RelVal::getSrcNodeID() const {
    return srcNodeIDVal->getValue<nodeID_t>();
}

nodeID_t RelVal::getDstNodeID() const {
    return dstNodeIDVal->getValue<nodeID_t>();
}

std::string RelVal::getLabelName() {
    return labelVal->getValue<std::string>();
}

std::string RelVal::toString() const {
    std::string result;
    result += "(" + srcNodeIDVal->toString() + ")";
    result += "-[label:" + labelVal->toString() + ", " + propertiesToString(properties) + "]->";
    result += "(" + dstNodeIDVal->toString() + ")";
    return result;
}

std::unique_ptr<RelVal> RelVal::copy() const {
    return std::make_unique<RelVal>(*this);
}

} // namespace common
} // namespace kuzu
