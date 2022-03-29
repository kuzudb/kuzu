#include "include/literal.h"

#include <cassert>

namespace graphflow {
namespace common {

Literal::Literal(const Literal& other) : dataType{other.dataType} {
    switch (dataType.typeID) {
    case BOOL: {
        val.booleanVal = other.val.booleanVal;
    } break;
    case INT64: {
        val.int64Val = other.val.int64Val;
    } break;
    case DOUBLE: {
        val.doubleVal = other.val.doubleVal;
    } break;
    case DATE: {
        val.dateVal = other.val.dateVal;
    } break;
    case TIMESTAMP: {
        val.timestampVal = other.val.timestampVal;
    } break;
    case INTERVAL: {
        val.intervalVal = other.val.intervalVal;
    } break;
    case STRING: {
        strVal = other.strVal;
    } break;
    case LIST: {
        listVal = other.listVal;
    } break;
    default:
        assert(false);
    }
}

} // namespace common
} // namespace graphflow
