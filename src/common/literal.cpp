#include "src/common/include/literal.h"

#include <cassert>

namespace graphflow {
namespace common {

Literal& Literal::operator=(const Literal& other) {
    dataType = other.dataType;
    switch (dataType) {
    case BOOL: {
        val.booleanVal = other.val.booleanVal;
    } break;
    case INT32: {
        val.int32Val = other.val.int32Val;
    } break;
    case DOUBLE: {
        val.doubleVal = other.val.doubleVal;
    } break;
    case STRING: {
        strVal = other.strVal;
    } break;
    default:
        assert(false);
    }

    return *this;
}

string Literal::toString() const {
    switch (dataType) {
    case BOOL:
        return val.booleanVal == TRUE ? "True" : (val.booleanVal == FALSE ? "False" : "");
    case INT32:
    case INT64:
        return to_string(val.int32Val);
    case DOUBLE:
        return to_string(val.doubleVal);
    case STRING:
        return strVal;
    case NODE:
        return to_string(val.nodeID.label) + ":" + to_string(val.nodeID.offset);
    default:
        assert(false);
    }
}
} // namespace common
} // namespace graphflow
