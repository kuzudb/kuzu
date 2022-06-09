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

struct Upper {
public:
    static inline void operation(
        gf_string_t& input, gf_string_t& result, ValueVector& resultValueVector) {
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
} // namespace graphflow
