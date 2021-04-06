#pragma once

#include <cstdint>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {
namespace operation {

struct And {
    static inline uint8_t operation(
        uint8_t left, uint8_t right, bool isLeftNull, bool isRightNull) {
        if (left == FALSE || right == FALSE) {
            return FALSE;
        }
        if (isLeftNull || isRightNull) {
            return NULL_BOOL;
        }
        return TRUE;
    }
};

struct Or {
    static inline uint8_t operation(
        uint8_t left, uint8_t right, bool isLeftNull, bool isRightNull) {
        if (left == TRUE || right == TRUE) {
            return TRUE;
        }
        if (isLeftNull || isRightNull) {
            return NULL_BOOL;
        }
        return FALSE;
    }
};

struct Xor {
    static inline uint8_t operation(
        uint8_t left, uint8_t right, bool isLeftNull, bool isRightNull) {
        if (isLeftNull || isRightNull) {
            return NULL_BOOL;
        }
        return left ^ right;
    }
};

struct Not {
    static inline uint8_t operation(uint8_t operand, bool isNull) {
        if (isNull) {
            return NULL_BOOL;
        }
        return !operand;
    }
};

} // namespace operation
} // namespace common
} // namespace graphflow
