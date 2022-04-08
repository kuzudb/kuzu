#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Upper {
    template<class A, class R>
    static inline void operation(A& operand, bool isLeftNull, R& result);

    static void upper(char* str, uint32_t len) {
        for (auto i = 0u; i < len; i++) {
            str[i] = toupper(str[i]);
        }
    }
};

template<>
inline void Upper::operation(gf_string_t& operand, bool isOperandNull, gf_string_t& result) {
    assert(!isOperandNull);
    auto len = operand.len;
    if (len <= gf_string_t::SHORT_STR_LENGTH) {
        memcpy(result.prefix, operand.prefix, len);
        upper((char*)result.prefix, len);
    } else {
        auto buffer = reinterpret_cast<char*>(result.overflowPtr);
        memcpy(buffer, operand.getData(), len);
        upper(buffer, len);
        memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
    }
    result.len = len;
}

template<>
inline void Upper::operation(Value& operand, bool isOperandNull, Value& result) {
    assert(operand.dataType.typeID == STRING);
    result.dataType.typeID = STRING;
    operation(operand.val.strVal, isOperandNull, result.val.strVal);
}

} // namespace operation
} // namespace function
} // namespace graphflow
