#include "src/common/include/types.h"

#include <iostream>

namespace graphflow {
namespace common {

DataType getDataType(const std::string& dataTypeString) {
    if (0 == dataTypeString.compare("LABEL")) {
        return LABEL;
    } else if (0 == dataTypeString.compare("NODE")) {
        return NODE;
    } else if (0 == dataTypeString.compare("REL")) {
        return REL;
    } else if (0 == dataTypeString.compare("INT")) {
        return INT;
    } else if (0 == dataTypeString.compare("DOUBLE")) {
        return DOUBLE;
    } else if (0 == dataTypeString.compare("BOOLEAN")) {
        return BOOL;
    } else if (0 == dataTypeString.compare("STRING")) {
        return STRING;
    }
    throw invalid_argument("Cannot parse dataType: " + dataTypeString);
}

size_t getDataTypeSize(DataType dataType) {
    switch (dataType) {
    case INT:
        return sizeof(int32_t);
    case DOUBLE:
        return sizeof(double_t);
    case BOOL:
        return sizeof(uint8_t);
    case STRING:
        return sizeof(gf_string_t);
    case NODE:
        return NUM_BYTES_PER_NODE_ID;
    case LABEL:
        return sizeof(label_t);
    default:
        throw invalid_argument("Cannot infer the size of dataType.");
    }
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
