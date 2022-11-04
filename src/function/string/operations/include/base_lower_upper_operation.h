#pragma once

#include <cassert>
#include <cstring>

#include "third_party/utf8proc/include/utf8proc.h"

#include "src/common/include/vector/value_vector.h"
#include "src/common/types/include/gf_string.h"

using namespace graphflow::common;
using namespace graphflow::utf8proc;

namespace graphflow {
namespace function {
namespace operation {

struct BaseLowerUpperOperation {

    static inline void operation(
        gf_string_t& input, gf_string_t& result, ValueVector& resultValueVector, bool isUpper) {
        uint32_t resultLen = getResultLen((char*)input.getData(), input.len, isUpper);
        result.len = resultLen;
        if (resultLen <= gf_string_t::SHORT_STR_LENGTH) {
            convertCase((char*)result.prefix, input.len, (char*)input.getData(), isUpper);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(result.len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            convertCase(buffer, input.len, (char*)input.getData(), isUpper);
            memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
        }
    }

private:
    static uint32_t getResultLen(char* inputStr, uint32_t inputLen, bool isUpper);
    static void convertCase(char* result, uint32_t len, char* input, bool toUpper);
};
} // namespace operation
} // namespace function
} // namespace graphflow
