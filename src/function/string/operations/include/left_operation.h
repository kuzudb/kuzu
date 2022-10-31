#pragma once

#include <cassert>
#include <cstring>

#include "length_operation.h"
#include "substr_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::utf8proc;

namespace graphflow {
namespace function {
namespace operation {

struct Left {
public:
    static inline void operation(
        gf_string_t& left, int64_t& right, gf_string_t& result, ValueVector& resultValueVector) {
        int64_t leftLen;
        Length::operation(left, leftLen);
        int64_t len = (right > 0) ? min(leftLen, right) : max(leftLen + right, 0l);
        SubStr::operation(left, 1, len, result, resultValueVector);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
