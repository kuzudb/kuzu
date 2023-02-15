#pragma once

#include "common/type_utils.h"

namespace kuzu {
namespace function {
namespace operation {

struct Offset {
    static inline void operation(common::internalID_t& input, int64_t& result) {
        result = input.offset;
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
