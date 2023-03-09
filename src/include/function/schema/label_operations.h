#pragma once

#include "common/type_utils.h"

namespace kuzu {
namespace function {
namespace operation {

struct Label {
    static inline void operation(common::internalID_t& left, common::ku_list_t& right,
        common::ku_string_t& result, common::ValueVector& resultVector) {
        assert(left.tableID < right.size);
        auto& value = ((common::ku_string_t*)right.overflowPtr)[left.tableID];
        if (!common::ku_string_t::isShortString(value.len)) {
            result.overflowPtr =
                (uint64_t)resultVector.getOverflowBuffer().allocateSpace(value.len);
        }
        result.set(value);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
