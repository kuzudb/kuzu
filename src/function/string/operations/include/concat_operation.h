#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Concat {
    template<class A, class B, class R>
    static inline void operation(
        A& left, B& right, R& result, bool isLeftNull, bool isRightNull, ValueVector& valueVector);

    static void concat(const char* left, uint32_t leftLen, const char* right, uint32_t rightLen,
        gf_string_t& result, ValueVector& resultValueVector) {
        auto len = leftLen + rightLen;
        if (len <= gf_string_t::SHORT_STR_LENGTH /* concat's result is short */) {
            memcpy(result.prefix, left, leftLen);
            memcpy(result.prefix + leftLen, right, rightLen);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            memcpy(buffer, left, leftLen);
            memcpy(buffer + leftLen, right, rightLen);
            memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
        }
        result.len = len;
    }
};

template<>
inline void Concat::operation(gf_string_t& left, gf_string_t& right, gf_string_t& result,
    bool isLeftNull, bool isRightNull, ValueVector& resultValueVector) {
    assert(!isLeftNull && !isRightNull);
    concat((const char*)left.getData(), left.len, (const char*)right.getData(), right.len, result,
        resultValueVector);
}

template<>
inline void Concat::operation(string& left, string& right, gf_string_t& result, bool isLeftNull,
    bool isRightNull, ValueVector& resultValueVector) {
    assert(!isLeftNull && !isRightNull);
    concat(left.c_str(), left.length(), right.c_str(), right.length(), result, resultValueVector);
}

} // namespace operation
} // namespace function
} // namespace graphflow
