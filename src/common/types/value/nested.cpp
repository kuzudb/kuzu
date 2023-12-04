#include "common/types/value/nested.h"

#include "common/exception/runtime.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace common {

uint32_t NestedVal::getChildrenSize(const Value* val) {
    return val->childrenSize;
}

Value* NestedVal::getChildVal(const Value* val, uint32_t idx) {
    if (idx > val->childrenSize) {
        throw RuntimeException("NestedVal::getChildPointer index out of bound.");
    }
    return val->children[idx].get();
}

} // namespace common
} // namespace kuzu
