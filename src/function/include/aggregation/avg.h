#pragma once

#include "src/common/include/operations/arithmetic_operations.h"
#include "src/function/include/aggregation/aggregation_function.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace function {

template<typename T>
struct AvgFunction {

    struct AvgState : public AggregationState<T> {
        uint64_t numValues;
        bool isSet;
    };

    static void initialize(uint8_t* state_) {
        auto state = reinterpret_cast<AvgState*>(state_);
        state->numValues = 0;
        state->isSet = false;
    }

    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        assert(input && count == input->state->selectedSize);
        auto state = reinterpret_cast<AvgState*>(state_);
        auto inputValues = (T*)input->values;
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < count; i++) {
                if (!state->isSet) {
                    state->val = inputValues[input->state->selectedPositions[i]];
                    state->isSet = true;
                } else {
                    Add::template operation<T, T, T>(
                        state->val, inputValues[input->state->selectedPositions[i]], state->val);
                }
            }
            state->numValues += count;
        } else {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (!input->isNull(pos)) {
                    if (!state->isSet) {
                        state->val = inputValues[input->state->selectedPositions[i]];
                        state->isSet = true;
                    } else {
                        Add::template operation<T, T, T>(state->val,
                            inputValues[input->state->selectedPositions[i]], state->val);
                    }
                    state->numValues++;
                }
            }
        }
    }

    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto state = reinterpret_cast<AvgState*>(state_);
        auto otherState = reinterpret_cast<AvgState*>(otherState_);
        Add::template operation<T, T, T>(state->val, otherState->val, state->val);
        state->numValues += otherState->numValues;
    }

    static void finalize(uint8_t* inputState_, uint8_t* finalState_) {
        auto inputState = reinterpret_cast<AvgState*>(inputState_);
        auto finalState = reinterpret_cast<AvgFunction<double_t>::AvgState*>(finalState_);
        finalState->val = (double_t)inputState->val / (double_t)inputState->numValues;
    }
};

} // namespace function
} // namespace graphflow
