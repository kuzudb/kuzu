#include "processor/result/value.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Value::set(const uint8_t* value) {
    switch (dataType.typeID) {
    case INT64: {
        val.int64Val = *((int64_t*)value);
    } break;
    case BOOL: {
        val.booleanVal = *((bool*)value);
    } break;
    case DOUBLE: {
        val.doubleVal = *((double*)value);
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
    case NODE_ID: {
        val.nodeIDVal = *((nodeID_t*)value);
    } break;
    case STRING: {
        stringVal = ((ku_string_t*)value)->getAsString();
    } break;
    case LIST: {
        listVal = convertKUListToVector(*(ku_list_t*)value);
    } break;
    default:
        throw RuntimeException("Data type " + Types::dataTypeToString(dataType.typeID) +
                               " is not supported for ResultValue::set.");
    }
}

string Value::toString() const {
    if (isNull) {
        return "";
    }
    switch (dataType.typeID) {
    case BOOL:
        return TypeUtils::toString(val.booleanVal);
    case INT64:
        return TypeUtils::toString(val.int64Val);
    case DOUBLE:
        return TypeUtils::toString(val.doubleVal);
    case DATE:
        return TypeUtils::toString(val.dateVal);
    case TIMESTAMP:
        return TypeUtils::toString(val.timestampVal);
    case INTERVAL:
        return TypeUtils::toString(val.intervalVal);
    case NODE_ID:
        return TypeUtils::toString(val.nodeIDVal);
    case STRING:
        return stringVal;
    case LIST: {
        string result = "[";
        for (auto i = 0u; i < listVal.size(); ++i) {
            result += listVal[i]->toString();
            result += (i == listVal.size() - 1 ? "]" : ",");
        }
        return result;
    }
    case NODE: {
        return nodeVal->toString();
    }
    case REL: {
        return relVal->toString();
    }
    default:
        throw RuntimeException("Data type " + Types::dataTypeToString(dataType) +
                               " is not supported for ResultValue::toString.");
    }
}

void Value::validateType(common::DataTypeID typeID) const {
    if (typeID != dataType.typeID) {
        throw common::RuntimeException(
            common::StringUtils::string_format("Cannot get %s value from the %s result value.",
                common::Types::dataTypeToString(typeID).c_str(),
                common::Types::dataTypeToString(dataType.typeID).c_str()));
    }
}

vector<unique_ptr<Value>> Value::convertKUListToVector(ku_list_t& list) const {
    vector<unique_ptr<Value>> listResultValue;
    auto numBytesPerElement = Types::getDataTypeSize(*dataType.childType);
    for (auto i = 0; i < list.size; i++) {
        auto childResultValue = make_unique<Value>(*dataType.childType);
        childResultValue->set(
            reinterpret_cast<uint8_t*>(list.overflowPtr + i * numBytesPerElement));
        listResultValue.emplace_back(std::move(childResultValue));
    }
    return listResultValue;
}

string NodeOrRelVal::propertiesToString() const {
    std::string result = "{";
    for (auto i = 0u; i < properties.size(); ++i) {
        auto& [name, value] = properties[i];
        result += name + ":" + value->toString();
        result += (i == properties.size() - 1 ? "" : ", ");
    }
    result += "}";
    return result;
}

string NodeVal::toString() const {
    std::string result = "(";
    result += idVal->toString();
    result += ":" + labelVal->toString() + " ";
    result += propertiesToString();
    result += ")";
    return result;
}

string RelVal::toString() const {
    std::string result;
    result += "(" + srcNodeIDVal->toString() + ")";
    result += "-[" + propertiesToString() + "]->";
    result += "(" + dstNodeIDVal->toString() + ")";
    return result;
}

} // namespace processor
} // namespace kuzu
