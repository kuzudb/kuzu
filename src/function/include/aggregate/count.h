#pragma once

#include "src/function/include/aggregate/base_count.h"

namespace graphflow {
namespace function {

struct CountFunction : public BaseCountFunction {

    static void update(uint8_t* state_, ValueVector* input, uint64_t multiplicity) {
        auto state = reinterpret_cast<CountState*>(state_);
        if (input->state->isFlat()) {
            auto pos = input->state->getPositionOfCurrIdx();
            if (!input->isNull(pos)) {
                state->val += multiplicity;
            }
        } else {
            if (input->hasNoNullsGuarantee()) {
                for (auto i = 0u; i < input->state->selectedSize; ++i) {
                    state->val += multiplicity;
                }
            } else {
                for (auto i = 0u; i < input->state->selectedSize; ++i) {
                    auto pos = input->state->selectedPositions[i];
                    if (!input->isNull(pos)) {
                        state->val += multiplicity;
                    }
                }
            }
        }
    }
};

} // namespace function
} // namespace graphflow
