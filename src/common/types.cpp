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
}

} // namespace utils
} // namespace graphflow
