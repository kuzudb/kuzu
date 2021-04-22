#include "src/common/include/value.h"

#include <cassert>

using namespace std;

namespace graphflow {
namespace common {

string Value::toString() const {
    switch (dataType) {
    case BOOL:
        return primitive.boolean_ ? "True" : "False";
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
        assert(false);
    }
}

} // namespace common
} // namespace graphflow
