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
    case INT64: {
        val.int64Val = other.val.int64Val;
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
        return TypeUtils::toString(val.booleanVal);
    case INT64:
        return TypeUtils::toString(val.int64Val);
    case DOUBLE:
        return TypeUtils::toString(val.doubleVal);
    case STRING:
        return strVal;
    case NODE:
        return TypeUtils::toString(val.nodeID);
    default:
        assert(false);
    }
}
} // namespace common
} // namespace graphflow
