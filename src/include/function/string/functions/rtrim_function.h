#pragma once

#include <cassert>
#include <cstring>

#include "base_str_function.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct Rtrim {
    static inline void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector) {
        BaseStrOperation::operation(input, result, resultValueVector, rtrim);
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

} // namespace function
} // namespace kuzu
