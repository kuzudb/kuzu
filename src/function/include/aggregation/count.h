#pragma once
#include "src/function/include/aggregation/aggregation_function.h"

namespace graphflow {
namespace function {

template<bool IS_COUNT_STAR>
struct CountFunction {

    struct CountState : public AggregationState {
        uint64_t val = 0;

        inline uint64_t getValSize() const override { return sizeof(*this); }
        inline uint8_t* getFinalVal() const override { return (uint8_t*)&val; }
    };

    static unique_ptr<AggregationState> initialize() {
        auto state = make_unique<CountState>();
        state->isNull = false;
        return state;
    }

    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        auto state = reinterpret_cast<CountState*>(state_);
        if constexpr (IS_COUNT_STAR) {
            assert(input == nullptr);
            state->val += count;
        } else {
            assert(input && count == input->state->selectedSize);
            if (input->hasNoNullsGuarantee()) {
                state->val += count;
            } else {
                for (auto i = 0u; i < count; i++) {
                    state->val += (1 - input->isNull(input->state->selectedPositions[i]));
                }
            }
        }
    }

    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto state = reinterpret_cast<CountState*>(state_);
        auto otherState = reinterpret_cast<CountState*>(otherState_);
        state->val += otherState->val;
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace graphflow
