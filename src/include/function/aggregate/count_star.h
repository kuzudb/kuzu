#pragma once

#include "base_count.h"

namespace kuzu {
namespace function {

struct CountStarFunction : public BaseCountFunction {

    static void updateAll(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity) {
        auto state = reinterpret_cast<CountState*>(state_);
        assert(input == nullptr);
        state->count += multiplicity;
    }

    static void updatePos(
        uint8_t* state_, common::ValueVector* input, uint64_t multiplicity, uint32_t pos) {
        auto state = reinterpret_cast<CountState*>(state_);
        assert(input == nullptr);
        state->count += multiplicity;
    }
};

} // namespace function
} // namespace kuzu
