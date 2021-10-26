#pragma once

#include "src/common/include/operations/arithmetic_operations.h"
#include "src/function/include/aggregation/aggregation_function.h"

using namespace std;
using namespace graphflow::common::operation;

namespace graphflow {
namespace function {

template<typename T>
struct AvgFunction {

    struct AvgState : public AggregationState {
        double_t val;
        uint64_t numValues = 0;

        inline uint64_t getValSize() const override { return sizeof(*this); }
        inline uint8_t* getFinalVal() const override { return (uint8_t*)&val; }
    };

    static unique_ptr<AggregationState> initialize() { return make_unique<AvgState>(); }

    static void update(uint8_t* state_, ValueVector* input, uint64_t count) {
        assert(input && !input->state->isFlat());
        count = input->state->selectedSize;
        auto state = reinterpret_cast<AvgState*>(state_);
        auto inputValues = (T*)input->values;
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < count; i++) {
                if (state->isNull) {
                    state->val = (double_t)inputValues[input->state->selectedPositions[i]];
                    state->isNull = false;
                } else {
                    Add::template operation<double_t, T, double_t>(
                        state->val, inputValues[input->state->selectedPositions[i]], state->val);
                }
            }
            state->numValues = state->numValues + count;
        } else {
            for (auto i = 0u; i < count; i++) {
                auto pos = input->state->selectedPositions[i];
                if (!input->isNull(pos)) {
                    if (state->isNull) {
                        state->val = inputValues[input->state->selectedPositions[i]];
                        state->isNull = false;
                    } else {
                        Add::template operation<double_t, T, double_t>(state->val,
                            inputValues[input->state->selectedPositions[i]], state->val);
                    }
                    state->numValues = state->numValues + 1;
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
        if (state->isNull) {
            state->val = otherState->val;
            state->isNull = false;
        } else {
            Add::template operation<double_t, double_t, double_t>(
                state->val, otherState->val, state->val);
        }
        state->numValues = state->numValues + otherState->numValues;
    }

    static void finalize(uint8_t* state_) {
        auto state = reinterpret_cast<AvgState*>(state_);
        if (!state->isNull) {
            state->val = state->val / (double_t)state->numValues;
        }
    }
};

} // namespace function
} // namespace graphflow
