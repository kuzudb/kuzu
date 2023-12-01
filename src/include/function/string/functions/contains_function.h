#pragma once

#include "common/types/ku_string.h"
#include "function/string/functions/find_function.h"

namespace kuzu {
namespace function {

struct Contains {
    static inline void operation(
        common::ku_string_t& left, common::ku_string_t& right, uint8_t& result) {
        auto lStr = left.getAsString();
        auto rStr = right.getAsString();
        int64_t pos;
        Find::operation(left, right, pos);
        result = (pos != 0);
    }
};

} // namespace function
} // namespace kuzu
