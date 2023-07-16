#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct Upper {
public:
    static inline void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector) {
        BaseLowerUpperFunction::operation(input, result, resultValueVector, true /* isUpper */);
    }
};

} // namespace function
} // namespace kuzu
