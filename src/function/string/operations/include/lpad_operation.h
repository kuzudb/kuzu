#pragma once

#include <cassert>
#include <cstring>

#include "pad_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Lpad : PadOperation {
public:
    static inline void operation(gf_string_t& src, int64_t count, gf_string_t& characterToPad,
        gf_string_t& result, ValueVector& resultValueVector) {
        PadOperation::operation(
            src, count, characterToPad, result, resultValueVector, lpadOperation);
    }

    static void lpadOperation(gf_string_t& result, gf_string_t& src, gf_string_t& characterToPad) {
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
} // namespace graphflow
