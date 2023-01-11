#pragma once

#include "common/type_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct Label {
    static inline void operation(
        nodeID_t& left, ku_list_t& right, ku_string_t& result, ValueVector& resultVector) {
        assert(left.tableID < right.size);
        auto& value = ((ku_string_t*)right.overflowPtr)[left.tableID];
        if (!ku_string_t::isShortString(value.len)) {
            result.overflowPtr =
                (uint64_t)resultVector.getOverflowBuffer().allocateSpace(value.len);
        }
        result.set(value);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
