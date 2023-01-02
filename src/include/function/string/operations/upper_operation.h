#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct Upper {
public:
    static inline void operation(
        ku_string_t& input, ku_string_t& result, ValueVector& resultValueVector) {
        BaseLowerUpperOperation::operation(input, result, resultValueVector, true /* isUpper */);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
