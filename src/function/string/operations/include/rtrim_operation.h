#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Rtrim {
    template<class A, class R>
    static inline void operation(A& operand, bool isLeftNull, R& result);

    static uint32_t rtrim(char* data, uint32_t len) {
        auto counter = len - 1;
        for (; counter >= 0; counter--) {
            if (!isspace(data[counter])) {
                break;
            }
        }
        return counter + 1;
    }
};

template<>
inline void Rtrim::operation(gf_string_t& operand, bool isOperandNull, gf_string_t& result) {
    assert(!isOperandNull);
    auto len = operand.len;
    result.len =
        rtrim(len <= gf_string_t::SHORT_STR_LENGTH ? (char*)operand.prefix :
                                                     reinterpret_cast<char*>(operand.overflowPtr),
            len);
    if (result.len <= gf_string_t::SHORT_STR_LENGTH) {
        memcpy(result.prefix, operand.prefix, result.len);
    } else {
        auto buffer = reinterpret_cast<char*>(operand.overflowPtr);
        memcpy(buffer, operand.getData(), result.len);
        memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
    }
}

template<>
inline void Rtrim::operation(Value& operand, bool isOperandNull, Value& result) {
    assert(operand.dataType.typeID == STRING);
    result.dataType.typeID = STRING;
    operation(operand.val.strVal, isOperandNull, result.val.strVal);
}

} // namespace operation
} // namespace function
} // namespace graphflow
