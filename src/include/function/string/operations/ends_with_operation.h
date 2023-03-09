#pragma once

#include "common/types/ku_string.h"
#include "function/string/operations/find_operation.h"

namespace kuzu {
namespace function {
namespace operation {

struct EndsWith {
    static inline void operation(
        common::ku_string_t& left, common::ku_string_t& right, uint8_t& result) {
        int64_t pos = 0;
        Find::operation(left, right, pos);
        result = (pos == left.len - right.len + 1);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
