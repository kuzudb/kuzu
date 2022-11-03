#pragma once

#include <cstring>

#include "base_lowerupper_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Lower {
public:
    static inline void operation(
        gf_string_t& input, gf_string_t& result, ValueVector& resultValueVector) {
        BaseLowerUpperOperation::operation(input, result, resultValueVector, false /* isUpper */);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
