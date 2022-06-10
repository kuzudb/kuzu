#pragma once

#include <cassert>
#include <cstring>

#include "base_str_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Ltrim {
    static inline void operation(
        gf_string_t& input, gf_string_t& result, ValueVector& resultValueVector) {
        BaseStrOperation::operation(input, result, resultValueVector, ltrim);
    }

    static uint32_t ltrim(char* data, uint32_t len) {
        auto counter = 0u;
        for (; counter < len; counter++) {
            if (!isspace(data[counter])) {
                break;
            }
        }
        memcpy(data, data + counter, len - counter);
        return len - counter;
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
