#pragma once

#include "src/common/include/operations/arithmetic_operations.h"
#include "src/function/include/aggregate/aggregate_function.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace function {

template<typename T>
struct SumFunction {

    struct SumState : public AggregateState {
        T val;

        inline uint64_t getValSize() const override { return sizeof(*this); }
        inline uint8_t* getFinalVal() const override { return (uint8_t*)&val; }
    };

    static unique_ptr<AggregateState> initialize() { return make_unique<SumState>(); }

    static void update(uint8_t* state_, ValueVector* input, uint64_t multiplicity) {
        auto state = reinterpret_cast<SumState*>(state_);
        if (input->state->isFlat()) {
            auto pos = input->state->getPositionOfCurrIdx();
            if (!input->isNull(pos)) {
                updateSingleValue(state, input, pos, multiplicity);
            }
        } else {
            if (input->hasNoNullsGuarantee()) {
                for (auto i = 0u; i < input->state->selectedSize; ++i) {
                    auto pos = input->state->selectedPositions[i];
                    updateSingleValue(state, input, pos, multiplicity);
                }
            } else {
                for (auto i = 0u; i < input->state->selectedSize; ++i) {
                    auto pos = input->state->selectedPositions[i];
                    if (!input->isNull(pos)) {
                        updateSingleValue(state, input, pos, multiplicity);
                    }
                }
            }
        }
    }

    static void updateSingleValue(
        SumState* state, ValueVector* input, uint32_t pos, uint64_t multiplicity) {
        auto inputValues = (T*)input->values;
        for (auto j = 0u; j < multiplicity; ++j) {
            if (state->isNull) {
                state->val = inputValues[pos];
                state->isNull = false;
            } else {
                Add::operation<T, T, T>(state->val, inputValues[pos], state->val,
                    false /* isLeftNull */, false /* isRightNull */);
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
            Add::operation<T, T, T>(state->val, otherState->val, state->val, false /* isLeftNull */,
                false /* isRightNull */);
        }
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace graphflow
