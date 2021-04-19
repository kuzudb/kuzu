#include "src/common/include/value.h"

#include <cassert>

using namespace std;

namespace graphflow {
namespace common {

string Value::toString() const {
    switch (dataType) {
    case BOOL:
        return primitive.boolean_ ? "true" : "false";
    case INT32:
    case INT64:
        return to_string(primitive.integer_);
    case DOUBLE:
        return to_string(primitive.double_);
    case STRING:
        return str;
    case NODE:
        return to_string(nodeID.label) + ":" + to_string(nodeID.offset);
    default:
        assert(false); // should never happen.
    }
}

string Value::toValueAndType() const {
    switch (dataType) {
    case BOOL:
        return primitive.boolean_ ? "Bool(true)" : "Bool(false)";
    case INT32:
        return "Integer(" + to_string(primitive.integer_) + ")";
    case DOUBLE:
        return "Double(" + to_string(primitive.double_) + ")";
    case STRING:
        return "String(" + str + ")";
    case NODE:
        return "Node(" + to_string(nodeID.label) + ":" + to_string(nodeID.offset) + ")";
    default:
        assert(false); // should never happen.
    }
}

} // namespace common
} // namespace graphflow
