#pragma once

#include "src/common/include/operations/arithmetic_operations.h"
#include "src/function/include/aggregation/aggregation_function.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace function {

template<typename T>
struct AvgFunction {

    struct AvgState : public AggregationState {
        uint64_t numValues = 0;
    };

    static unique_ptr<AggregationState> initialize() {
        auto state = make_unique<AvgState>();
        state->val = make_unique<uint8_t[]>(sizeof(double_t));
        return state;
    }

    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        assert(input && !input->state->isFlat());
        count = input->state->selectedSize;
        auto state = reinterpret_cast<AvgState*>(state_);
        auto stateValue = (double_t*)state->val.get();
        auto inputValues = (T*)input->values;
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < count; i++) {
                if (state->isNull) {
                    *stateValue = inputValues[input->state->selectedPositions[i]];
                    state->isNull = false;
                } else {
                    Add::template operation<double_t, T, double_t>(
                        *stateValue, inputValues[input->state->selectedPositions[i]], *stateValue);
                }
            }
            state->numValues += count;
        } else {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (!input->isNull(pos)) {
                    if (state->isNull) {
                        *stateValue = inputValues[input->state->selectedPositions[i]];
                        state->isNull = false;
                    } else {
                        Add::template operation<double_t, T, double_t>(*stateValue,
                            inputValues[input->state->selectedPositions[i]], *stateValue);
                    }
                    state->numValues++;
                }
            }
        }
    }

    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto otherState = reinterpret_cast<AvgState*>(otherState_);
        if (otherState->isNull) {
            return;
        }
        auto state = reinterpret_cast<AvgState*>(state_);
        auto stateValue = (double_t*)state->val.get();
        auto otherStateValue = (double_t*)otherState->val.get();
        if (state->isNull) {
            *stateValue = *otherStateValue;
            state->isNull = false;
        } else {
            Add::template operation<double_t, double_t, double_t>(
                *stateValue, *otherStateValue, *stateValue);
        }
        state->numValues += otherState->numValues;
    }

    static void finalize(uint8_t* state_) {
        auto state = reinterpret_cast<AvgState*>(state_);
        auto stateValue = (double_t*)state->val.get();
        if (!state->isNull) {
            *stateValue = *stateValue / (double_t)state->numValues;
        }
    }
};

} // namespace function
} // namespace graphflow
