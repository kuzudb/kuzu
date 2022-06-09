#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_string.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct ArrayExtract {
    static inline void operation(gf_string_t& str, int64_t& idx, gf_string_t& result) {
        auto pos = idx > 0 ? min(idx, (int64_t)str.len) : max(str.len + idx, (int64_t)0) + 1;
        result.set((char*)(str.getData() + pos - 1), 1 /* length */);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
