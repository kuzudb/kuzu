#pragma once

#include "common/types/internal_id_t.h"

namespace kuzu {
namespace function {

struct Offset {
    static inline void operation(common::internalID_t& input, int64_t& result) {
        result = input.offset;
    }
};

} // namespace function
} // namespace kuzu
