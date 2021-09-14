#pragma once
#include "src/function/include/aggregation/aggregation_function.h"

namespace graphflow {
namespace function {

template<bool IS_COUNT_STAR>
struct CountFunction {

    struct CountState : public AggregationState {};

    static unique_ptr<AggregationState> initialize() {
        auto state = make_unique<AggregationState>();
        state->val = make_unique<uint8_t[]>(sizeof(uint64_t));
        *((uint64_t*)state->val.get()) = 0;
        state->isNull = false;
        return state;
    }

    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        auto state = reinterpret_cast<CountState*>(state_);
        auto stateValue = (uint64_t*)state->val.get();
        if constexpr (IS_COUNT_STAR) {
            assert(input == nullptr);
            *stateValue += count;
        } else {
            assert(input && count == input->state->selectedSize);
            if (input->hasNoNullsGuarantee()) {
                *stateValue += count;
            } else {
                for (auto i = 0u; i < count; i++) {
                    *stateValue += (1 - input->isNull(input->state->selectedPositions[i]));
                }
            }
        }
    }

    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto state = reinterpret_cast<CountState*>(state_);
        auto stateValue = (uint64_t*)state->val.get();
        auto otherState = reinterpret_cast<CountState*>(otherState_);
        auto otherStateValue = (uint64_t*)otherState->val.get();
        *stateValue += *otherStateValue;
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace graphflow
