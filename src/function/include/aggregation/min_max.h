#pragma once

#include "src/common/include/operations/comparison_operations.h"
#include "src/function/include/aggregation/aggregation_function.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace function {

template<typename T>
struct MinMaxFunction {

    struct MinMaxState : public AggregationState {};

    static unique_ptr<AggregationState> initialize() {
        auto state = make_unique<MinMaxState>();
        state->val = make_unique<uint8_t[]>(sizeof(T));
        return state;
    }

    template<class OP>
    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        assert(input);
        count = input->state->selectedSize;
        auto state = reinterpret_cast<MinMaxState*>(state_);
        auto stateValue = (T*)state->val.get();
        auto inputValues = (T*)input->values;
        uint8_t compare_result;
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (state->isNull) {
                    *stateValue = inputValues[pos];
                    state->isNull = false;
                } else {
                    OP::template operation<T, T>(inputValues[pos], *stateValue, compare_result);
                    *stateValue = compare_result == TRUE ? inputValues[pos] : *stateValue;
                }
            }
        } else {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (!input->isNull(pos)) {
                    if (state->isNull) {
                        *stateValue = inputValues[pos];
                        state->isNull = false;
                    } else {
                        OP::template operation<T, T>(inputValues[pos], *stateValue, compare_result);
                        *stateValue = compare_result == TRUE ? inputValues[pos] : *stateValue;
                    }
                }
            }
        }
    }

    template<class OP>
    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto otherState = reinterpret_cast<MinMaxState*>(otherState_);
        if (otherState->isNull) {
            return;
        }
        auto state = reinterpret_cast<MinMaxState*>(state_);
        auto stateValue = (T*)state->val.get();
        auto otherStateValue = (T*)otherState->val.get();
        if (state->isNull) {
            *stateValue = *otherStateValue;
            state->isNull = false;
        } else {
            uint8_t compareResult;
            OP::template operation<T, T>(*otherStateValue, *stateValue, compareResult);
            *stateValue = compareResult == 1 ? *otherStateValue : *stateValue;
        }
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace graphflow
