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

struct Rpad : PadOperation {
public:
    static inline void operation(gf_string_t& src, int64_t count, gf_string_t& characterToPad,
        gf_string_t& result, bool isSrcNull, bool isCountNull, bool isCharacterToPadNull,
        ValueVector& resultValueVector) {
        PadOperation::operation(src, count, characterToPad, result, isSrcNull, isCountNull,
            isCharacterToPadNull, resultValueVector, rpadOperation);
    }

    static void rpadOperation(gf_string_t& result, gf_string_t& src, gf_string_t& characterToPad) {
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
} // namespace graphflow
