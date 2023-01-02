#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"
#include "length_operation.h"
#include "substr_operation.h"

using namespace std;
using namespace kuzu::common;
using namespace kuzu::utf8proc;

namespace kuzu {
namespace function {
namespace operation {

struct Left {
public:
    static inline void operation(
        ku_string_t& left, int64_t& right, ku_string_t& result, ValueVector& resultValueVector) {
        int64_t leftLen;
        Length::operation(left, leftLen);
        int64_t len = (right > -1) ? min(leftLen, right) : max(leftLen + right, (int64_t)0);
        SubStr::operation(left, 1, len, result, resultValueVector);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
