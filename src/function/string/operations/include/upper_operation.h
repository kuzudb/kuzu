#pragma once

#include <cassert>
#include <cstring>

#include "base_str_operation.h"

#include "src/common/types/include/ku_string.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct Upper {
public:
    static inline void operation(
        ku_string_t& input, ku_string_t& result, ValueVector& resultValueVector) {
        BaseStrOperation::operation(input, result, resultValueVector, upperStr);
    }

private:
    static uint32_t upperStr(char* str, uint32_t len) {
        for (auto i = 0u; i < len; i++) {
            str[i] = toupper(str[i]);
        }
        return len;
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
