#pragma once

#include <cassert>
#include <cstring>

#include "substr_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Right {
public:
    static inline void operation(
        gf_string_t& left, int64_t& right, gf_string_t& result, ValueVector& resultValueVector) {
        auto totalCharCount = getLength(left);
        int64_t len;
        if (right > 0) {
            len = min(totalCharCount, (uint32_t)right);
        } else if ((totalCharCount + right) > 0) {
            len = (totalCharCount + right);
        } else {
            len = 0;
        }
        SubStr::operation(left, totalCharCount - len + 1, len, result, resultValueVector);
    }

    static uint32_t getLength(gf_string_t& input) {
        auto totalByteLength = input.len;
        auto inputString = input.getAsString();
        for (uint32_t i = 0; i < totalByteLength; i++) {
            if (inputString[i] & 0x80) {
                uint32_t length = 0;
                utf8proc_grapheme_callback(
                    inputString.c_str(), totalByteLength, [&](size_t start, size_t end) {
                        length++;
                        return true;
                    });
                return length;
            }
        }
        return input.len;
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
