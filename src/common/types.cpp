#include "src/common/include/types.h"

#include <iostream>

namespace graphflow {
namespace common {

DataType getDataType(const std::string &dataTypeString) {
    if (0 == dataTypeString.compare("NODE")) {
        return NODE;
    } else if (0 == dataTypeString.compare("REL")) {
        return REL;
    } else if (0 == dataTypeString.compare("INT")) {
        return INT;
    } else if (0 == dataTypeString.compare("DOUBLE")) {
        return DOUBLE;
    } else if (0 == dataTypeString.compare("BOOLEAN")) {
        return BOOLEAN;
    } else if (0 == dataTypeString.compare("STRING")) {
        return STRING;
    }
    throw invalid_argument("Cannot parse dataType.");
}

uint8_t getDataTypeSize(DataType dataType) {
    switch (dataType) {
    case INT:
        return sizeof(gfInt_t);
    case DOUBLE:
        return sizeof(gfDouble_t);
    case BOOLEAN:
        return sizeof(gfBool_t);
    case STRING:
        return sizeof(gfString_t);
    case NODE:
    case REL:
        return sizeof(gfNodeOffset_t);
    }
    throw invalid_argument("Cannot infer the size of the dataType.");
}

Cardinality getCardinality(const string &cardinalityString) {
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

Direction operator!(Direction &direction) {
    auto reverse = (FORWARD == direction) ? BACKWARD : FORWARD;
    return reverse;
}

} // namespace common
} // namespace graphflow
