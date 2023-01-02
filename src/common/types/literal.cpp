#include "common/types/literal.h"

#include <cassert>

namespace kuzu {
namespace common {

Literal::Literal(uint8_t* value, const DataType& dataType) : _isNull{false}, dataType{dataType} {
    switch (dataType.typeID) {
    case BOOL:
        val.booleanVal = *(bool*)value;
        break;
    case INT64:
        val.int64Val = *(int64_t*)value;
        break;
    case DOUBLE:
        val.doubleVal = *(double_t*)value;
        break;
    case NODE_ID:
        val.nodeID = *(nodeID_t*)value;
        break;
    case DATE:
        val.dateVal = *(date_t*)value;
        break;
    case TIMESTAMP:
        val.timestampVal = *(timestamp_t*)value;
        break;
    case INTERVAL:
        val.intervalVal = *(interval_t*)value;
        break;
    default:
        assert(false);
    }
}

Literal::Literal(const Literal& other) : dataType{other.dataType} {
    bind(other);
}

void Literal::bind(const Literal& other) {
    if (other._isNull) {
        _isNull = true;
        return;
    }
    _isNull = false;
    assert(dataType.typeID == other.dataType.typeID);
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
} // namespace kuzu
