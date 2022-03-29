#pragma once

#include "src/function/arithmetic/operations/include/arithmetic_operations.h"
#include "src/function/include/aggregate/aggregate_function.h"

using namespace graphflow::function::operation;

namespace graphflow {
namespace function {

template<typename T>
struct SumFunction {

    struct SumState : public AggregateState {
        inline uint64_t getStateSize() const override { return sizeof(*this); }
        inline uint8_t* getResult() const override { return (uint8_t*)&sum; }

        T sum;
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
                state->sum = inputValues[pos];
                state->isNull = false;
            } else {
                Add::operation(state->sum, inputValues[pos], state->sum, false /* isLeftNull */,
                    false /* isRightNull */);
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
            state->sum = otherState->sum;
            state->isNull = false;
        } else {
            Add::operation(state->sum, otherState->sum, state->sum, false /* isLeftNull */,
                false /* isRightNull */);
        }
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace graphflow
