#pragma once

#include <cassert>
#include <cstring>

#include "base_lower_upper_function.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct Lower {
public:
    static inline void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector) {
        BaseLowerUpperFunction::operation(input, result, resultValueVector, false /* isUpper */);
    }
};

} // namespace function
} // namespace kuzu
