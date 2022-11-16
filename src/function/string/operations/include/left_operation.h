#pragma once

#include <cassert>
#include <cstring>

#include "substr_operation.h"

#include "src/common/types/include/ku_string.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct Left {
public:
    static inline void operation(
        ku_string_t& left, int64_t& right, ku_string_t& result, ValueVector& resultValueVector) {
        auto len = right >= 0 ? min(left.len, (uint32_t)right) :
                                ((uint32_t)max(left.len + right, (int64_t)0));
        SubStr::operation(left, 1, len, result, resultValueVector);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
