#pragma once

#include "substr_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct EndsWith {
    static inline void operation(gf_string_t& left, gf_string_t& right, uint8_t& result) {
        auto len = right.len;
        gf_string_t substr_result;
        ValueVector ex{BOOL};
        SubStr::operation(left, left.len - len + 1, len, substr_result, ex);
        result = (substr_result == right);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
