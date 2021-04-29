#include "src/common/include/value.h"

#include <cassert>

using namespace std;

namespace graphflow {
namespace common {

Value& Value::operator=(const Value& other) {
    dataType = other.dataType;
    switch (dataType) {
    case BOOL: {
        primitive.booleanVal = other.primitive.booleanVal;
    } break;
    case INT32: {
        primitive.int32Val = other.primitive.int32Val;
    } break;
    case DOUBLE: {
        primitive.doubleVal = other.primitive.doubleVal;
    } break;
    case STRING: {
        strVal = other.strVal;
    } break;
    default:
        assert(false);
    }
}

void Value::castToString() {
    switch (dataType) {
    case BOOL: {
        strVal.set(to_string(primitive.booleanVal));
    } break;
    case INT32: {
        strVal.set(to_string(primitive.int32Val));
    } break;
    case DOUBLE: {
        strVal.set(to_string(primitive.doubleVal));
    } break;
    default:
        assert(false);
    }
    dataType = STRING;
}

string Value::toString() const {
    switch (dataType) {
    case BOOL:
        return primitive.booleanVal == TRUE ? "True" :
                                              (primitive.booleanVal == FALSE ? "False" : "");
    case INT32:
    case INT64:
        return to_string(primitive.int32Val);
    case DOUBLE:
        return to_string(primitive.doubleVal);
    case STRING:
        return gf_string_t::getAsString(strVal);
    case NODE:
        return to_string(nodeID.label) + ":" + to_string(nodeID.offset);
    default:
        assert(false);
    }
}

} // namespace common
} // namespace graphflow
