#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Length {
    template<class A, class R>
    static inline void operation(A& operand, bool isLeftNull, R& result);
};

template<>
inline void Length::operation(gf_string_t& operand, bool isOperandNull, int64_t& result) {
    assert(!isOperandNull);
    result = operand.len;
}

template<>
inline void Length::operation(Value& operand, bool isOperandNull, Value& result) {
    assert(operand.dataType.typeID == STRING);
    result.dataType.typeID = INT64;
    operation(operand.val.strVal, isOperandNull, result.val.int64Val);
}

} // namespace operation
} // namespace function
} // namespace graphflow
