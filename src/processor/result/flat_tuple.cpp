#include "processor/result/flat_tuple.h"

#include <iomanip>
#include <sstream>
#include <string>

#include "common/type_utils.h"
#include "common/utils.h"

namespace kuzu {
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
        stringVal = ((ku_string_t*)value)->getAsString();
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
        listVal = convertKUListToVector(*(ku_list_t*)value);
    } break;
    default:
        throw RuntimeException("Data type " + Types::dataTypeToString(valueType.typeID) +
                               " is not supported for ResultValue::set.");
    }
}

string ResultValue::toString() const {
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
            result += listVal[i].toString();
            result += (i == listVal.size() - 1 ? "]" : ",");
        }
        return result;
    }
    default:
        throw RuntimeException("Data type " + Types::dataTypeToString(dataType) +
                               " is not supported for ResultValue::toString.");
    }
}

vector<ResultValue> ResultValue::convertKUListToVector(ku_list_t& list) const {
    vector<ResultValue> listResultValue;
    auto numBytesPerElement = Types::getDataTypeSize(*dataType.childType);
    for (auto i = 0; i < list.size; i++) {
        ResultValue childResultValue{*dataType.childType};
        childResultValue.set(reinterpret_cast<uint8_t*>(list.overflowPtr + i * numBytesPerElement),
            *dataType.childType);
        listResultValue.emplace_back(std::move(childResultValue));
    }
    return listResultValue;
}

ResultValue* FlatTuple::getResultValue(uint32_t valIdx) {
    if (valIdx >= len()) {
        throw RuntimeException(StringUtils::string_format(
            "ValIdx is out of range. Number of values in flatTuple: %d, valIdx: %d.", len(),
            valIdx));
    }
    return resultValues[valIdx].get();
}

string FlatTuple::toString(const vector<uint32_t>& colsWidth, const string& delimiter) {
    ostringstream result;
    for (auto i = 0ul; i < resultValues.size(); i++) {
        string value = resultValues[i]->toString();
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
} // namespace kuzu
