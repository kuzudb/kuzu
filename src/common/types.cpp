#include "src/common/include/types.h"

#include <cstdlib>
#include <stdexcept>

#include "src/common/include/gf_string.h"
#include "src/common/include/value.h"

namespace graphflow {
namespace common {

DataType getDataType(const std::string& dataTypeString) {
    if ("LABEL" == dataTypeString) {
        return LABEL;
    } else if ("NODE" == dataTypeString) {
        return NODE;
    } else if ("REL" == dataTypeString) {
        return REL;
    } else if ("INT32" == dataTypeString) {
        return INT32;
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
    }
    throw invalid_argument("Cannot parse dataType: " + dataTypeString);
}

string dataTypeToString(DataType dataType) {
    return DataTypeNames[dataType];
}

bool isNumericalType(DataType dataType) {
    return dataType == INT32 || dataType == INT64 || dataType == DOUBLE;
}

size_t getDataTypeSize(DataType dataType) {
    switch (dataType) {
    case LABEL:
        return sizeof(label_t);
    case NODE:
        return NUM_BYTES_PER_NODE_ID;
    case INT32:
        return sizeof(int32_t);
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
    default:
        throw invalid_argument(
            "Cannot infer the size of dataType: " + dataTypeToString(dataType) + ".");
    }
}

int32_t convertToInt32(char* data) {
    return atoi(data);
};

double_t convertToDouble(char* data) {
    return atof(data);
};

uint8_t convertToBoolean(char* data) {
    static char* trueVal = (char*)"true";
    static char* falseVal = (char*)"false";
    if (0 == strcmp(data, trueVal)) {
        return 1;
    }
    if (0 == strcmp(data, falseVal)) {
        return 2;
    }
    throw invalid_argument("invalid boolean val.");
}

Direction operator!(Direction& direction) {
    return (FWD == direction) ? BWD : FWD;
}

} // namespace common
} // namespace graphflow
