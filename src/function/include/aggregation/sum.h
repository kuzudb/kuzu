#pragma once

#include "src/function/include/aggregation/aggregation_function.h"

namespace graphflow {
namespace function {

template<typename T>
struct SumFunction {

    struct SumState : public AggregationState {};

    static unique_ptr<AggregationState> initialize() {
        auto state = make_unique<SumState>();
        state->val = make_unique<uint8_t[]>(sizeof(T));
        return state;
    }

    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        assert(input && !input->state->isFlat());
        count = input->state->selectedSize;
        auto state = reinterpret_cast<SumState*>(state_);
        auto stateValue = (T*)state->val.get();
        auto inputValues = (T*)input->values;
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < count; i++) {
                if (state->isNull) {
                    *stateValue = inputValues[input->state->selectedPositions[i]];
                    state->isNull = false;
                } else {
                    Add::operation(
                        *stateValue, inputValues[input->state->selectedPositions[i]], *stateValue);
                }
            }
        } else {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (!input->isNull(pos)) {
                    if (state->isNull) {
                        *stateValue = inputValues[input->state->selectedPositions[i]];
                        state->isNull = false;
                    } else {
                        Add::operation(*stateValue, inputValues[input->state->selectedPositions[i]],
                            *stateValue);
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
        auto stateValue = (T*)state->val.get();
        auto otherStateValue = (T*)otherState->val.get();
        if (state->isNull) {
            *stateValue = *otherStateValue;
            state->isNull = false;
        } else {
            Add::operation(*stateValue, *otherStateValue, *stateValue);
        }
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace graphflow
