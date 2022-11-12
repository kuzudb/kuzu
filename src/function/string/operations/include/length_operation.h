#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/ku_string.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct Length {
    static inline void operation(ku_string_t& input, int64_t& result) { result = input.len; }
};

} // namespace operation
} // namespace function
} // namespace kuzu
