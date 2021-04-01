#include "src/common/include/types.h"

#include <cstdlib>

namespace graphflow {
namespace common {

DataType getDataType(const std::string& dataTypeString) {
    if (0 == dataTypeString.compare("LABEL")) {
        return LABEL;
    } else if (0 == dataTypeString.compare("NODE")) {
        return NODE;
    } else if (0 == dataTypeString.compare("REL")) {
        return REL;
    } else if (0 == dataTypeString.compare("INT32")) {
        return INT32;
    } else if (0 == dataTypeString.compare("INT64")) {
        return INT64;
    } else if (0 == dataTypeString.compare("DOUBLE")) {
        return DOUBLE;
    } else if (0 == dataTypeString.compare("BOOLEAN")) {
        return BOOL;
    } else if (0 == dataTypeString.compare("STRING")) {
        return STRING;
    }
    throw invalid_argument("Cannot parse dataType: " + dataTypeString);
}

string dataTypeToString(DataType dataType) {
    return DataTypeNames[dataType];
}

bool isNumericalType(DataType dataType) {
    switch (dataType) {
    case INT32:
    case INT64:
    case DOUBLE:
        return true;
    default:
        return false;
    }
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
    default:
        throw invalid_argument("Cannot infer the size of dataType.");
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

void gf_string_t::copyOverflowPtrFromPageCursor(const PageCursor& cursor) {
    memcpy(&overflowPtr, &cursor.idx, 6);
    memcpy(((uint8_t*)&overflowPtr) + 6, &cursor.offset, 2);
}

void gf_string_t::copyOverflowPtrToPageCursor(PageCursor& cursor) {
    cursor.idx = 0;
    memcpy(&cursor.idx, &overflowPtr, 6);
    memcpy(&cursor.offset, ((uint8_t*)&overflowPtr) + 6, 2);
}

Cardinality getCardinality(const string& cardinalityString) {
    if (0 == cardinalityString.compare("ONE_ONE")) {
        return ONE_ONE;
    } else if (0 == cardinalityString.compare("MANY_ONE")) {
        return MANY_ONE;
    } else if (0 == cardinalityString.compare("ONE_MANY")) {
        return ONE_MANY;
    } else if (0 == cardinalityString.compare("MANY_MANY")) {
        return MANY_MANY;
    }
    throw invalid_argument("Invalid cardinality string \"" + cardinalityString + "\"");
}

Direction operator!(Direction& direction) {
    auto reverse = (FWD == direction) ? BWD : FWD;
    return reverse;
}

} // namespace common
} // namespace graphflow
