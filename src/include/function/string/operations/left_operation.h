#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"
#include "length_operation.h"
#include "substr_operation.h"

namespace kuzu {
namespace function {
namespace operation {

struct Left {
public:
    static inline void operation(common::ku_string_t& left, int64_t& right,
        common::ku_string_t& result, common::ValueVector& resultValueVector) {
        int64_t leftLen;
        Length::operation(left, leftLen);
        int64_t len =
            (right > -1) ? std::min(leftLen, right) : std::max(leftLen + right, (int64_t)0);
        SubStr::operation(left, 1, len, result, resultValueVector);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
