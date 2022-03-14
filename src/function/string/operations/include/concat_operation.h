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
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull);
};

template<>
inline void Concat::operation(
    gf_string_t& left, gf_string_t& right, gf_string_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    auto len = left.len + right.len;
    if (len <= gf_string_t::SHORT_STR_LENGTH /* concat's result is short */) {
        memcpy(result.prefix, left.prefix, left.len);
        memcpy(result.prefix + left.len, right.prefix, right.len);
    } else {
        auto buffer = reinterpret_cast<char*>(result.overflowPtr);
        memcpy(buffer, left.getData(), left.len);
        memcpy(buffer + left.len, right.getData(), right.len);
        memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
    }
    result.len = len;
}

template<>
inline void Concat::operation(
    string& left, string& right, gf_string_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    auto len = left.length() + right.length();
    if (len <= gf_string_t::SHORT_STR_LENGTH /* concat's result is short */) {
        memcpy(result.prefix, left.c_str(), left.length());
        memcpy(result.prefix + left.length(), right.c_str(), right.length());
    } else {
        auto buffer = reinterpret_cast<char*>(result.overflowPtr);
        memcpy(buffer, left.c_str(), left.length());
        memcpy(buffer + left.length(), right.c_str(), right.length());
        memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
    }
    result.len = len;
}

} // namespace operation
} // namespace function
} // namespace graphflow
