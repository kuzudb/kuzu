#pragma once

#include "base_count.h"

namespace kuzu {
namespace function {

struct CountFunction : public BaseCountFunction {

    static void updateAll(uint8_t* state_, ValueVector* input, uint64_t multiplicity) {
        auto state = reinterpret_cast<CountState*>(state_);
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < input->state->selVector->selectedSize; ++i) {
                state->count += multiplicity;
            }
        } else {
            for (auto i = 0u; i < input->state->selVector->selectedSize; ++i) {
                auto pos = input->state->selVector->selectedPositions[i];
                if (!input->isNull(pos)) {
                    state->count += multiplicity;
                }
            }
        }
    }

    static inline void updatePos(
        uint8_t* state_, ValueVector* input, uint64_t multiplicity, uint32_t pos) {
        reinterpret_cast<CountState*>(state_)->count += multiplicity;
    }
};

} // namespace function
} // namespace kuzu
