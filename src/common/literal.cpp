#include "src/common/include/literal.h"

using namespace std;

namespace graphflow {
namespace common {

string Literal::toString() const {
    switch (type) {
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
        throw std::invalid_argument("Unsupported data type " + DataTypeNames[type]);
    }
}

} // namespace common
} // namespace graphflow
