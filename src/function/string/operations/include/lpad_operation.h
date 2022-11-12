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

struct Lpad : PadOperation {
public:
    static inline void operation(ku_string_t& src, int64_t count, ku_string_t& characterToPad,
        ku_string_t& result, ValueVector& resultValueVector) {
        PadOperation::operation(
            src, count, characterToPad, result, resultValueVector, lpadOperation);
    }

    static void lpadOperation(ku_string_t& result, ku_string_t& src, ku_string_t& characterToPad) {
        auto offset = 0u;
        if (result.len > src.len) {
            for (; offset < result.len - src.len; offset++) {
                memcpy((uint8_t*)result.getData() + offset, characterToPad.getData(), 1);
            }
        }
        memcpy((uint8_t*)result.getData() + offset, src.getData(), src.len);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
