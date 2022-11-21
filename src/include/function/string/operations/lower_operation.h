#pragma once

#include <cassert>
#include <cstring>

#include "base_lower_upper_operation.h"
#include "common/types/ku_string.h"

using namespace std;
using namespace kuzu::common;
using namespace kuzu::function::operation;

namespace kuzu {
namespace function {
namespace operation {

struct Lower {
public:
    static inline void operation(
        ku_string_t& input, ku_string_t& result, ValueVector& resultValueVector) {
        BaseLowerUpperOperation::operation(input, result, resultValueVector, false /* isUpper */);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
