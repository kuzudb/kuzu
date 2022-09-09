#pragma once

#include "src/common/types/include/gf_string.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct StartsWith {
    static inline void operation(gf_string_t& left, gf_string_t& right, uint8_t& result) {
        auto lStr = left.getAsString();
        auto rStr = right.getAsString();
        result = lStr.starts_with(rStr);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
