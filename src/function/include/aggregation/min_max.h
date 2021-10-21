#pragma once

#include "src/common/include/operations/comparison_operations.h"
#include "src/function/include/aggregation/aggregation_function.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace function {

template<typename T>
struct MinMaxFunction {

    struct MinMaxState : public AggregationState {
        T val;

        inline uint64_t getValSize() const override { return sizeof(*this); }
        inline uint8_t* getFinalVal() const override { return (uint8_t*)&val; }
    };

    static unique_ptr<AggregationState> initialize() { return make_unique<MinMaxState>(); }

    template<class OP>
    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        assert(input);
        count = input->state->selectedSize;
        auto state = reinterpret_cast<MinMaxState*>(state_);
        auto inputValues = (T*)input->values;
        uint8_t compare_result;
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (state->isNull) {
                    state->val = inputValues[pos];
                    state->isNull = false;
                } else {
                    OP::template operation<T, T>(inputValues[pos], state->val, compare_result);
                    state->val = compare_result == TRUE ? inputValues[pos] : state->val;
                }
            }
        } else {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (!input->isNull(pos)) {
                    if (state->isNull) {
                        state->val = inputValues[pos];
                        state->isNull = false;
                    } else {
                        OP::template operation<T, T>(inputValues[pos], state->val, compare_result);
                        state->val = compare_result == TRUE ? inputValues[pos] : state->val;
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
        if (state->isNull) {
            state->val = otherState->val;
            state->isNull = false;
        } else {
            uint8_t compareResult;
            OP::template operation<T, T>(otherState->val, state->val, compareResult);
            state->val = compareResult == 1 ? otherState->val : state->val;
        }
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace graphflow
