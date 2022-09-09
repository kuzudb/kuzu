#pragma once

#include "src/common/types/include/gf_string.h"
#include "src/function/string/operations/include/find_operation.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct EndsWith {
    static inline void operation(gf_string_t& left, gf_string_t& right, uint8_t& result) {
        int64_t pos = 0;
        Find::operation(left, right, pos);
        result = (pos == left.len - right.len + 1);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
