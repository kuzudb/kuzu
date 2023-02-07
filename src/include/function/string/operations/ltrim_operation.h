#pragma once

#include <cassert>
#include <cstring>

#include "base_str_operation.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace function {
namespace operation {

struct Ltrim {
    static inline void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector) {
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
} // namespace kuzu
