#pragma once

#include "src/function/include/aggregate/base_count.h"

namespace graphflow {
namespace function {

struct CountStarFunction : public BaseCountFunction {

    static void update(uint8_t* state_, ValueVector* input, uint64_t multiplicity) {
        auto state = reinterpret_cast<CountState*>(state_);
        assert(input == nullptr);
        state->val += multiplicity;
    }
};

} // namespace function
} // namespace graphflow
