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

struct Rtrim {
    static inline void operation(
        gf_string_t& input, bool isNull, gf_string_t& result, ValueVector& resultValueVector) {
        BaseStrOperation::operation(input, isNull, result, resultValueVector, rtrim);
    }

    static uint32_t rtrim(char* data, uint32_t len) {
        auto counter = len - 1;
        for (; counter >= 0; counter--) {
            if (!isspace(data[counter])) {
                break;
            }
        }
        return counter + 1;
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
