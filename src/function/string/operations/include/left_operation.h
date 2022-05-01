#pragma once

#include <cassert>
#include <cstring>

#include "substr_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Left {
public:
    static inline void operation(gf_string_t& left, int64_t& right, gf_string_t& result,
        bool isLeftNull, bool isRightNull, ValueVector& resultValueVector) {
        assert(!isLeftNull && !isRightNull);
        auto len = right > 0 ? min(left.len, (uint32_t)right) :
                               max(left.len + (uint32_t)right, (uint32_t)0u);
        SubStr::operation(left, 1, len, result, false /* isLeftNull*/, false /* isStartNull*/,
            false /*isLenNull */, resultValueVector);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
