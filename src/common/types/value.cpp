#include "common/types/value.h"

#include <cassert>

using namespace std;

namespace kuzu {
namespace common {

Value& Value::operator=(const Value& other) {
    dataType = other.dataType;
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
    case STRING: {
        val.strVal = other.val.strVal;
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
    default:
        assert(false);
    }
    return *this;
}

} // namespace common
} // namespace kuzu
