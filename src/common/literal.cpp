#include "src/common/include/literal.h"

#include "src/common/include/assert.h"
#include "src/common/include/date.h"

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
    case STRING: {
        strVal = other.strVal;
    } break;
    default:
        GF_ASSERT(false);
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
    case STRING:
        return strVal;
    default:
        GF_ASSERT(false);
    }
    // Should never happen. Add empty return to remove compilation warning.
    return string();
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
        strVal = Date::toString(val.dateVal);
    } break;
    default:
        GF_ASSERT(false);
    }
    dataType = STRING;
}

} // namespace common
} // namespace graphflow
