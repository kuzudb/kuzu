#pragma once

#include <cassert>
#include <cstring>

#include "base_lower_upper_operation.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace function {
namespace operation {

struct Lower {
public:
    static inline void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector) {
        BaseLowerUpperOperation::operation(input, result, resultValueVector, false /* isUpper */);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
