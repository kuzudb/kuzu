#pragma once

#include <cstdint>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {
namespace operation {

struct And {
    static inline uint8_t operation(uint8_t left, uint8_t right) { return left && right; }
};

struct Or {
    static inline uint8_t operation(uint8_t left, uint8_t right) { return left || right; }
};

struct Xor {
    static inline uint8_t operation(uint8_t left, uint8_t right) { return left ^ right; }
};

struct Not {
    static inline uint8_t operation(uint8_t operand) { return !operand; }
};

} // namespace operation
} // namespace common
} // namespace graphflow
