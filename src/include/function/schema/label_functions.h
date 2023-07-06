#pragma once

#include "common/type_utils.h"
#include "function/list/functions/list_extract_function.h"

namespace kuzu {
namespace function {

struct Label {
    static inline void operation(common::internalID_t& left, common::list_entry_t& right,
        common::ku_string_t& result, common::ValueVector& leftVector,
        common::ValueVector& rightVector, common::ValueVector& resultVector) {
        assert(left.tableID < right.size);
        ListExtract::operation(right, left.tableID + 1 /* listExtract requires 1-based index */,
            result, rightVector, leftVector, resultVector);
    }
};

} // namespace function
} // namespace kuzu
