#pragma once

#include <cstdint>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {
namespace operation {

struct And {
    static inline void operation(
        uint8_t left, uint8_t right, uint8_t& result, bool isLeftNull, bool isRightNull) {
        if (left == FALSE || right == FALSE) {
            result = FALSE;
        } else {
            result = isLeftNull || isRightNull ? NULL_BOOL : TRUE;
        }
    }
};

struct Or {
    static inline void operation(
        uint8_t left, uint8_t right, uint8_t& result, bool isLeftNull, bool isRightNull) {
        if (left == TRUE || right == TRUE) {
            result = TRUE;
        } else {
            result = isLeftNull || isRightNull ? NULL_BOOL : FALSE;
        }
    }
};

struct Xor {
    static inline void operation(
        uint8_t left, uint8_t right, uint8_t& result, bool isLeftNull, bool isRightNull) {
        result = (isLeftNull || isRightNull) ? NULL_BOOL : left ^ right;
    }
};

struct Not {
    static inline void operation(uint8_t operand, bool isNull, uint8_t& result) {
        result = isNull ? NULL_BOOL : !operand;
    }
};

} // namespace operation
} // namespace common
} // namespace graphflow
