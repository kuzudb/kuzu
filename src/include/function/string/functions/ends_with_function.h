#pragma once

#include "common/types/ku_string.h"
#include "function/string/functions/find_function.h"

namespace kuzu {
namespace function {

struct EndsWith {
    static inline void operation(
        common::ku_string_t& left, common::ku_string_t& right, uint8_t& result) {
        int64_t pos = 0;
        Find::operation(left, right, pos);
        result = (pos == left.len - right.len + 1);
    }
};

} // namespace function
} // namespace kuzu
