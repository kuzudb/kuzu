#pragma once

#include "src/function/include/aggregation/aggregation_function.h"

namespace graphflow {
namespace function {

template<typename T>
struct SumFunction {

    struct SumState : public AggregationState<T> {
        bool isSet;
    };

    static void initialize(uint8_t* state_) {
        auto state = reinterpret_cast<SumState*>(state_);
        state->isSet = false;
    }

    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        assert(input && count == input->state->selectedSize);
        auto state = reinterpret_cast<SumState*>(state_);
        auto inputValues = (T*)input->values;
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < count; i++) {
                if (!state->isSet) {
                    state->val = inputValues[input->state->selectedPositions[i]];
                    state->isSet = true;
                } else {
                    Add::operation(
                        state->val, inputValues[input->state->selectedPositions[i]], state->val);
                }
            }
        } else {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (!input->isNull(pos)) {
                    if (!state->isSet) {
                        state->val = inputValues[input->state->selectedPositions[i]];
                        state->isSet = true;
                    } else {
                        Add::operation(state->val, inputValues[input->state->selectedPositions[i]],
                            state->val);
                    }
                }
            }
        }
    }

    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto state = reinterpret_cast<SumState*>(state_);
        auto otherState = reinterpret_cast<SumState*>(otherState_);
        Add::operation(state->val, otherState->val, state->val);
    }

    static void finalize(uint8_t* inputState_, uint8_t* finalState_) {
        auto inputState = reinterpret_cast<SumState*>(inputState_);
        auto finalState = reinterpret_cast<SumState*>(finalState_);
        finalState->val = inputState->val;
    }
};

} // namespace function
} // namespace graphflow
