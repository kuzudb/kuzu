#include "src/common/include/value.h"

#include <cassert>

#include "src/common/include/date.h"
#include "src/common/include/timestamp.h"
#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

Value& Value::operator=(const Value& other) {
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
        val.strVal = other.val.strVal;
    } break;
    case DATE: {
        val.dateVal = other.val.dateVal;
    } break;
    case TIMESTAMP: {
        val.timestampVal = other.val.timestampVal;
    } break;
    default:
        assert(false);
    }

    return *this;
}

string Value::toString() const {
    switch (dataType) {
    case BOOL:
        return TypeUtils::toString(val.booleanVal);
    case INT64:
        return TypeUtils::toString(val.int64Val);
    case DOUBLE:
        return TypeUtils::toString(val.doubleVal);
    case STRING:
        return val.strVal.getAsString();
    case NODE:
        return TypeUtils::toString(val.nodeID);
    case DATE:
        return Date::toString(val.dateVal);
    case TIMESTAMP:
        return Timestamp::toString(val.timestampVal);
    default:
        assert(false);
    }
}

} // namespace common
} // namespace graphflow
