#pragma once

#include <cassert>

#include "src/common/types/include/gf_string.h"
#include "src/function/string/operations/include/find_operation.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Contains {
    static inline void operation(gf_string_t& left, gf_string_t& right, uint8_t& result) {
        auto lStr = left.getAsString();
        auto rStr = right.getAsString();
        int64_t pos;
        Find::operation(left, right, pos);
        result = (pos != 0);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
