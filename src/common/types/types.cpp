#include "include/types.h"

#include <stdexcept>

#include "include/types_include.h"
#include "include/value.h"

namespace graphflow {
namespace common {

DataType Types::getDataType(const std::string& dataTypeString) {
    if ("LABEL" == dataTypeString) {
        return LABEL;
    } else if ("NODE" == dataTypeString) {
        return NODE;
    } else if ("REL" == dataTypeString) {
        return REL;
    } else if ("INT64" == dataTypeString) {
        return INT64;
    } else if ("DOUBLE" == dataTypeString) {
        return DOUBLE;
    } else if ("BOOLEAN" == dataTypeString) {
        return BOOL;
    } else if ("STRING" == dataTypeString) {
        return STRING;
    } else if ("DATE" == dataTypeString) {
        return DATE;
    } else if ("TIMESTAMP" == dataTypeString) {
        return TIMESTAMP;
    } else if ("INTERVAL" == dataTypeString) {
        return INTERVAL;
    } else {
        throw invalid_argument("Cannot parse dataType: " + dataTypeString);
    }
}

string Types::dataTypeToString(DataType dataType) {
    return DataTypeNames[dataType];
}

bool Types::isNumericalType(DataType dataType) {
    return dataType == INT64 || dataType == DOUBLE;
}

size_t Types::getDataTypeSize(DataType dataType) {
    switch (dataType) {
    case LABEL:
        return sizeof(label_t);
    case NODE:
        return sizeof(nodeID_t);
    case INT64:
        return sizeof(int64_t);
    case DOUBLE:
        return sizeof(double_t);
    case BOOL:
        return sizeof(uint8_t);
    case STRING:
        return sizeof(gf_string_t);
    case UNSTRUCTURED:
        return sizeof(Value);
    case DATE:
        return sizeof(date_t);
    case TIMESTAMP:
        return sizeof(timestamp_t);
    case INTERVAL:
        return sizeof(interval_t);
    case LIST:
        return sizeof(gf_list_t);
    default:
        throw invalid_argument(
            "Cannot infer the size of dataType: " + dataTypeToString(dataType) + ".");
    }
}

Direction operator!(Direction& direction) {
    return (FWD == direction) ? BWD : FWD;
}

} // namespace common
} // namespace graphflow
