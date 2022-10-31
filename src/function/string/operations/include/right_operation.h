#pragma once

#include <cassert>
#include <cstring>

#include "length_operation.h"
#include "substr_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Right {
public:
    static inline void operation(
        gf_string_t& left, int64_t& right, gf_string_t& result, ValueVector& resultValueVector) {
        int64_t leftLen;
        Length::operation(left, leftLen);
        int64_t len = (right > 0) ? min(leftLen, right) : max(leftLen + right, (int64_t)0);
        SubStr::operation(left, leftLen - len + 1, len, result, resultValueVector);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
