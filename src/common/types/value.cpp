#include "common/types/value.h"

#include "common/string_utils.h"

namespace kuzu {
namespace common {

void Value::setDataType(const DataType& dataType_) {
    assert(dataType.typeID == ANY);
    dataType = dataType_;
}

DataType Value::getDataType() const {
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

Value Value::createNullValue(DataType dataType) {
    return Value(std::move(dataType));
}

Value Value::createDefaultValue(const DataType& dataType) {
    switch (dataType.typeID) {
    case INT64:
        return Value((int64_t)0);
    case INT32:
        return Value((int32_t)0);
    case INT16:
        return Value((int16_t)0);
    case BOOL:
        return Value(true);
    case DOUBLE:
        return Value((double_t)0);
    case DATE:
        return Value(date_t());
    case TIMESTAMP:
        return Value(timestamp_t());
    case INTERVAL:
        return Value(interval_t());
    case INTERNAL_ID:
        return Value(nodeID_t());
    case STRING:
        return Value(std::string(""));
    case FLOAT:
        return Value((float_t)0);
    case VAR_LIST:
    case FIXED_LIST:
        return Value(dataType, std::vector<std::unique_ptr<Value>>{});
    default:
        throw RuntimeException("Data type " + Types::dataTypeToString(dataType) +
                               " is not supported for Value::createDefaultValue");
    }
}

Value::Value(bool val_) : dataType{BOOL}, isNull_{false} {
    val.booleanVal = val_;
}

Value::Value(int16_t val_) : dataType{INT16}, isNull_{false} {
    val.int16Val = val_;
}

Value::Value(int32_t val_) : dataType{INT32}, isNull_{false} {
    val.int32Val = val_;
}

Value::Value(int64_t val_) : dataType{INT64}, isNull_{false} {
    val.int64Val = val_;
}

Value::Value(float_t val_) : dataType{FLOAT}, isNull_{false} {
    val.floatVal = val_;
}

Value::Value(double val_) : dataType{DOUBLE}, isNull_{false} {
    val.doubleVal = val_;
}

Value::Value(date_t val_) : dataType{DATE}, isNull_{false} {
    val.dateVal = val_;
}

Value::Value(kuzu::common::timestamp_t val_) : dataType{TIMESTAMP}, isNull_{false} {
    val.timestampVal = val_;
}

Value::Value(kuzu::common::interval_t val_) : dataType{INTERVAL}, isNull_{false} {
    val.intervalVal = val_;
}

Value::Value(kuzu::common::internalID_t val_) : dataType{INTERNAL_ID}, isNull_{false} {
    val.internalIDVal = val_;
}

Value::Value(const char* val_) : dataType{STRING}, isNull_{false} {
    strVal = std::string(val_);
}

Value::Value(const std::string& val_) : dataType{STRING}, isNull_{false} {
    strVal = val_;
}

Value::Value(DataType dataType, std::vector<std::unique_ptr<Value>> vals)
    : dataType{std::move(dataType)}, isNull_{false} {
    listVal = std::move(vals);
}

Value::Value(std::unique_ptr<NodeVal> val_) : dataType{NODE}, isNull_{false} {
    nodeVal = std::move(val_);
}

Value::Value(std::unique_ptr<RelVal> val_) : dataType{REL}, isNull_{false} {
    relVal = std::move(val_);
}

Value::Value(DataType dataType, const uint8_t* val_)
    : dataType{std::move(dataType)}, isNull_{false} {
    copyValueFrom(val_);
}

Value::Value(const Value& other) : dataType{other.dataType}, isNull_{other.isNull_} {
    copyValueFrom(other);
}

void Value::copyValueFrom(const uint8_t* value) {
    switch (dataType.typeID) {
    case INT64: {
        val.int64Val = *((int64_t*)value);
    } break;
    case INT32: {
        val.int32Val = *((int32_t*)value);
    } break;
    case INT16: {
        val.int16Val = *((int16_t*)value);
    } break;
    case BOOL: {
        val.booleanVal = *((bool*)value);
    } break;
    case DOUBLE: {
        val.doubleVal = *((double*)value);
    } break;
    case FLOAT: {
        val.floatVal = *((float_t*)value);
    } break;
    case DATE: {
        val.dateVal = *((date_t*)value);
    } break;
    case TIMESTAMP: {
        val.timestampVal = *((timestamp_t*)value);
    } break;
    case INTERVAL: {
        val.intervalVal = *((interval_t*)value);
    } break;
    case INTERNAL_ID: {
        val.internalIDVal = *((nodeID_t*)value);
    } break;
    case STRING: {
        strVal = ((ku_string_t*)value)->getAsString();
    } break;
    case VAR_LIST: {
        listVal = convertKUVarListToVector(*(ku_list_t*)value);
    } break;
    case FIXED_LIST: {
        listVal = convertKUFixedListToVector(value);
    } break;
    default:
        throw RuntimeException(
            "Data type " + Types::dataTypeToString(dataType) + " is not supported for Value::set");
    }
}

void Value::copyValueFrom(const Value& other) {
    if (other.isNull()) {
        isNull_ = true;
        return;
    }
    isNull_ = false;
    assert(dataType == other.dataType);
    switch (dataType.typeID) {
    case BOOL: {
        val.booleanVal = other.val.booleanVal;
    } break;
    case INT64: {
        val.int64Val = other.val.int64Val;
    } break;
    case INT32: {
        val.int32Val = other.val.int32Val;
    } break;
    case INT16: {
        val.int16Val = other.val.int16Val;
    } break;
    case DOUBLE: {
        val.doubleVal = other.val.doubleVal;
    } break;
    case FLOAT: {
        val.floatVal = other.val.floatVal;
    } break;
    case DATE: {
        val.dateVal = other.val.dateVal;
    } break;
    case TIMESTAMP: {
        val.timestampVal = other.val.timestampVal;
    } break;
    case INTERVAL: {
        val.intervalVal = other.val.intervalVal;
    } break;
    case INTERNAL_ID: {
        val.internalIDVal = other.val.internalIDVal;
    } break;
    case STRING: {
        strVal = other.strVal;
    } break;
    case VAR_LIST:
    case FIXED_LIST: {
        for (auto& value : other.listVal) {
            listVal.push_back(value->copy());
        }
    } break;
    case NODE: {
        nodeVal = other.nodeVal->copy();
    } break;
    case REL: {
        relVal = other.relVal->copy();
    } break;
    default:
        throw NotImplementedException("Value::Value(const Value&) for type " +
                                      Types::dataTypeToString(dataType) + " is not implemented.");
    }
}

const std::vector<std::unique_ptr<Value>>& Value::getListValReference() const {
    return listVal;
}

std::string Value::toString() const {
    if (isNull_) {
        return "";
    }
    switch (dataType.typeID) {
    case BOOL:
        return TypeUtils::toString(val.booleanVal);
    case INT64:
        return TypeUtils::toString(val.int64Val);
    case INT32:
        return TypeUtils::toString(val.int32Val);
    case INT16:
        return TypeUtils::toString(val.int16Val);
    case DOUBLE:
        return TypeUtils::toString(val.doubleVal);
    case FLOAT:
        return TypeUtils::toString(val.floatVal);
    case DATE:
        return TypeUtils::toString(val.dateVal);
    case TIMESTAMP:
        return TypeUtils::toString(val.timestampVal);
    case INTERVAL:
        return TypeUtils::toString(val.intervalVal);
    case INTERNAL_ID:
        return TypeUtils::toString(val.internalIDVal);
    case STRING:
        return strVal;
    case VAR_LIST:
    case FIXED_LIST: {
        std::string result = "[";
        for (auto i = 0u; i < listVal.size(); ++i) {
            result += listVal[i]->toString();
            if (i != listVal.size() - 1) {
                result += ",";
            }
        }
        result += "]";
        return result;
    }
    case NODE:
        return nodeVal->toString();
    case REL:
        return relVal->toString();
    default:
        throw NotImplementedException("Value::toString for type " +
                                      Types::dataTypeToString(dataType) + " is not implemented.");
    }
}

Value::Value() : dataType{ANY}, isNull_{true} {}

Value::Value(DataType dataType) : dataType{std::move(dataType)}, isNull_{true} {}

void Value::validateType(DataTypeID typeID) const {
    validateType(DataType(typeID));
}

void Value::validateType(const DataType& type) const {
    if (type != dataType) {
        throw RuntimeException(
            StringUtils::string_format("Cannot get {} value from the {} result value.",
                Types::dataTypeToString(type), Types::dataTypeToString(dataType)));
    }
}

std::vector<std::unique_ptr<Value>> Value::convertKUVarListToVector(ku_list_t& list) const {
    std::vector<std::unique_ptr<Value>> listResultValue;
    auto numBytesPerElement = Types::getDataTypeSize(*dataType.childType);
    for (auto i = 0; i < list.size; i++) {
        auto childValue = std::make_unique<Value>(Value::createDefaultValue(*dataType.childType));
        childValue->copyValueFrom(
            reinterpret_cast<uint8_t*>(list.overflowPtr + i * numBytesPerElement));
        listResultValue.emplace_back(std::move(childValue));
    }
    return listResultValue;
}

std::vector<std::unique_ptr<Value>> Value::convertKUFixedListToVector(
    const uint8_t* fixedList) const {
    std::vector<std::unique_ptr<Value>> fixedListResultVal{dataType.fixedNumElementsInList};
    auto numBytesPerElement = Types::getDataTypeSize(*dataType.childType);
    switch (dataType.childType->typeID) {
    case common::DataTypeID::INT64: {
        putValuesIntoVector<int64_t>(fixedListResultVal, fixedList, numBytesPerElement);
    } break;
    case common::DataTypeID::INT32: {
        putValuesIntoVector<int32_t>(fixedListResultVal, fixedList, numBytesPerElement);
    } break;
    case common::DataTypeID::INT16: {
        putValuesIntoVector<int16_t>(fixedListResultVal, fixedList, numBytesPerElement);
    } break;
    case common::DataTypeID::DOUBLE: {
        putValuesIntoVector<double_t>(fixedListResultVal, fixedList, numBytesPerElement);
    } break;
    case common::DataTypeID::FLOAT: {
        putValuesIntoVector<float_t>(fixedListResultVal, fixedList, numBytesPerElement);
    } break;
    default:
        assert(false);
    }
    return fixedListResultVal;
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
