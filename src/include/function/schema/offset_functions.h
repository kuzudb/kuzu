#pragma once

#include "common/type_utils.h"

namespace kuzu {
namespace function {

struct Offset {
    static inline void operation(common::internalID_t& input, int64_t& result) {
        result = input.offset;
    }
};

} // namespace function
} // namespace kuzu
