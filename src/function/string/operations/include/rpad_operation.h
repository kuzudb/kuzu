#pragma once

#include <cassert>
#include <cstring>

#include "pad_operation.h"

#include "src/common/types/include/ku_string.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct Rpad : PadOperation {
public:
    static inline void operation(ku_string_t& src, int64_t count, ku_string_t& characterToPad,
        ku_string_t& result, ValueVector& resultValueVector) {
        PadOperation::operation(
            src, count, characterToPad, result, resultValueVector, rpadOperation);
    }

    static void rpadOperation(ku_string_t& result, ku_string_t& src, ku_string_t& characterToPad) {
        auto offset = 0u;
        memcpy((uint8_t*)result.getData(), src.getData(), src.len);
        offset += src.len;
        for (; offset < result.len; offset++) {
            memcpy((uint8_t*)result.getData() + offset, characterToPad.getData(), 1);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
