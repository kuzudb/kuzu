#include "src/processor/include/physical_plan/result/flat_tuple.h"

#include <iomanip>
#include <sstream>
#include <string>

#include "src/common/include/type_utils.h"

namespace graphflow {
namespace processor {

void ResultValue::set(const uint8_t* value, DataType& valueType) {
    switch (valueType.typeID) {
    case INT64: {
        val.int64Val = *((int64_t*)value);
    } break;
    case BOOL: {
        val.booleanVal = *((bool*)value);
    } break;
    case DOUBLE: {
        val.doubleVal = *((double*)value);
    } break;
    case STRING: {
        stringVal = ((gf_string_t*)value)->getAsString();
    } break;
    case UNSTRUCTURED: {
        setFromUnstructuredValue(*(Value*)value);
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
    case LIST: {
        listVal = convertGFListToVector(*(gf_list_t*)value);
    } break;
    default:
        assert(false);
    }
}

string ResultValue::to_string() const {
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
    case STRING:
        return stringVal;
    case DATE:
        return TypeUtils::toString(val.dateVal);
    case TIMESTAMP:
        return TypeUtils::toString(val.timestampVal);
    case INTERVAL:
        return TypeUtils::toString(val.intervalVal);
    case LIST: {
        string result = "[";
        for (auto i = 0u; i < listVal.size(); ++i) {
            result += listVal[i].to_string();
            result += (i == listVal.size() - 1 ? "]" : ",");
        }
        return result;
    }
    default:
        assert(false);
    }
}

vector<ResultValue> ResultValue::convertGFListToVector(gf_list_t& list) const {
    vector<ResultValue> listResultValue;
    auto numBytesPerElement = Types::getDataTypeSize(*dataType.childType);
    for (auto i = 0; i < list.size; i++) {
        ResultValue childResultValue{*dataType.childType};
        childResultValue.set(reinterpret_cast<uint8_t*>(list.overflowPtr + i * numBytesPerElement),
            *dataType.childType);
        listResultValue.emplace_back(move(childResultValue));
    }
    return listResultValue;
}

void ResultValue::setFromUnstructuredValue(Value& value) {
    dataType = value.dataType;
    switch (value.dataType.typeID) {
    case INT64: {
        set((uint8_t*)&value.val.int64Val, value.dataType);
    } break;
    case BOOL: {
        set((uint8_t*)&value.val.booleanVal, value.dataType);
    } break;
    case DOUBLE: {
        set((uint8_t*)&value.val.doubleVal, value.dataType);
    } break;
    case STRING: {
        set((uint8_t*)&value.val.strVal, value.dataType);
    } break;
    case NODE_ID: {
        set((uint8_t*)&value.val.nodeID, value.dataType);
    } break;
    case DATE: {
        set((uint8_t*)&value.val.dateVal, value.dataType);
    } break;
    case TIMESTAMP: {
        set((uint8_t*)&value.val.timestampVal, value.dataType);
    } break;
    case INTERVAL: {
        set((uint8_t*)&value.val.intervalVal, value.dataType);
    } break;
    case LIST: {
        set((uint8_t*)&value.val.listVal, value.dataType);
    } break;
    default:
        assert(false);
    }
}

string FlatTuple::toString(const vector<uint32_t>& colsWidth, const string& delimiter) {
    ostringstream result;
    for (auto i = 0ul; i < resultValues.size(); i++) {
        string value = resultValues[i]->to_string();
        if (colsWidth[i] != 0) {
            value = " " + value + " ";
        }
        result << left << setw((int)colsWidth[i]) << setfill(' ') << value;
        if (i != resultValues.size() - 1) {
            result << delimiter;
        }
    }
    return result.str();
}

} // namespace processor
} // namespace graphflow
