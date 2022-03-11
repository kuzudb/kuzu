#include "include/literal.h"

#include <cassert>

#include "include/type_utils.h"

namespace graphflow {
namespace common {

Literal::Literal(const Literal& other) : dataType{other.dataType} {
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
    default:
        assert(false);
    }
}

string Literal::toString() const {
    switch (dataType) {
    case BOOL:
        return TypeUtils::toString(val.booleanVal);
    case INT64:
        return TypeUtils::toString(val.int64Val);
    case DOUBLE:
        return TypeUtils::toString(val.doubleVal);
    case NODE:
        return TypeUtils::toString(val.nodeID);
    case DATE:
        return Date::toString(val.dateVal);
    case TIMESTAMP:
        return Timestamp::toString(val.timestampVal);
    case INTERVAL:
        return Interval::toString(val.intervalVal);
    case STRING:
        return strVal;
    default:
        assert(false);
    }
}

void Literal::castToString() {
    switch (dataType) {
    case BOOL: {
        strVal = TypeUtils::toString(val.booleanVal);
    } break;
    case INT64: {
        strVal = TypeUtils::toString(val.int64Val);
    } break;
    case DOUBLE: {
        strVal = TypeUtils::toString(val.doubleVal);
    } break;
    case DATE: {
        strVal = TypeUtils::toString(val.dateVal);
    } break;
    case TIMESTAMP: {
        strVal = TypeUtils::toString(val.timestampVal);
    } break;
    default:
        assert(false);
    }
    dataType = STRING;
}

} // namespace common
} // namespace graphflow
