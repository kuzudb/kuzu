#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"

namespace kuzu {
namespace function {
namespace operation {

struct Upper {
public:
    static inline void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector) {
        BaseLowerUpperOperation::operation(input, result, resultValueVector, true /* isUpper */);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
