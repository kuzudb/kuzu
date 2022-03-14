#pragma once

#include <cassert>

#include "src/common/types/include/gf_string.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct StartsWith {
    template<class A, class B>
    static inline void operation(
        A& left, B& right, uint8_t& result, bool isLeftNull, bool isRightNull) {
        assert(false);
    }
};

template<>
void StartsWith::operation(
    gf_string_t& left, gf_string_t& right, uint8_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    auto lStr = left.getAsString();
    auto rStr = right.getAsString();
    result = lStr.starts_with(rStr);
}

} // namespace operation
} // namespace function
} // namespace graphflow
