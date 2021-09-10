#pragma once

#include "src/common/include/operations/comparison_operations.h"
#include "src/function/include/aggregation/aggregation_function.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace function {

template<typename T>
struct MinMaxFunction {

    struct MinMaxState : public AggregationState<T> {
        bool hasNull;
        bool isSet;
    };

    static void initialize(uint8_t* state_) {
        auto state = reinterpret_cast<MinMaxState*>(state_);
        state->hasNull = false;
        state->isSet = false;
    }

    template<class OP>
    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        assert(input && count == input->state->selectedSize);
        auto state = reinterpret_cast<MinMaxState*>(state_);
        auto inputValues = (T*)input->values;
        uint8_t compare_result;
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < count; i++) {
                auto inputVal = inputValues[input->state->selectedPositions[i]];
                if (!state->isSet) {
                    state->val = inputVal;
                    state->isSet = true;
                } else {
                    OP::template operation<T, T>(inputVal, state->val, compare_result);
                    state->val = compare_result == TRUE ? inputVal : state->val;
                }
            }
        } else {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (!input->isNull(pos)) {
                    auto inputVal = inputValues[input->state->selectedPositions[i]];
                    if (!state->isSet) {
                        state->val = inputVal;
                        state->isSet = true;
                    } else {
                        OP::template operation<T, T>(inputVal, state->val, compare_result);
                        state->val = compare_result == TRUE ? inputVal : state->val;
                    }
                }
            }
        }
    }

    template<class OP>
    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto state = reinterpret_cast<MinMaxState*>(state_);
        auto otherState = reinterpret_cast<MinMaxState*>(otherState_);
        uint8_t compareResult;
        OP::template operation<T, T>(otherState->val, state->val, compareResult);
        state->val = compareResult == 1 ? otherState->val : state->val;
        state->hasNull |= otherState->hasNull;
    }

    static void finalize(uint8_t* inputState_, uint8_t* finalState_) {
        auto inputState = reinterpret_cast<MinMaxState*>(inputState_);
        auto finalState = reinterpret_cast<MinMaxState*>(finalState_);
        finalState->val = inputState->val;
        finalState->hasNull = inputState->hasNull;
    }
};

} // namespace function
} // namespace graphflow
