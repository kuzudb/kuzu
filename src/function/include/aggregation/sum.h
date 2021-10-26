#pragma once

#include "src/function/include/aggregation/aggregation_function.h"

namespace graphflow {
namespace function {

template<typename T>
struct SumFunction {

    struct SumState : public AggregationState {
        T val;

        inline uint64_t getValSize() const override { return sizeof(*this); }
        inline uint8_t* getFinalVal() const override { return (uint8_t*)&val; }
    };

    static unique_ptr<AggregationState> initialize() { return make_unique<SumState>(); }

    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        assert(input && !input->state->isFlat());
        count = input->state->selectedSize;
        auto state = reinterpret_cast<SumState*>(state_);
        auto inputValues = (T*)input->values;
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < count; i++) {
                if (state->isNull) {
                    state->val = inputValues[input->state->selectedPositions[i]];
                    state->isNull = false;
                } else {
                    Add::operation(
                        state->val, inputValues[input->state->selectedPositions[i]], state->val);
                }
            }
        } else {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (!input->isNull(pos)) {
                    if (state->isNull) {
                        state->val = inputValues[input->state->selectedPositions[i]];
                        state->isNull = false;
                    } else {
                        Add::operation(state->val, inputValues[input->state->selectedPositions[i]],
                            state->val);
                    }
                }
            }
        }
    }

    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto otherState = reinterpret_cast<SumState*>(otherState_);
        if (otherState->isNull) {
            return;
        }
        auto state = reinterpret_cast<SumState*>(state_);
        if (state->isNull) {
            state->val = otherState->val;
            state->isNull = false;
        } else {
            Add::operation(state->val, otherState->val, state->val);
        }
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace graphflow
