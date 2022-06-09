#pragma once

#include "aggregate_function.h"

#include "src/function/arithmetic/operations/include/arithmetic_operations.h"

using namespace std;
using namespace graphflow::function::operation;

namespace graphflow {
namespace function {

template<typename T>
struct AvgFunction {

    struct AvgState : public AggregateState {
        inline uint32_t getStateSize() const override { return sizeof(*this); }
        inline uint8_t* getResult() const override { return (uint8_t*)&avg; }

        T sum;
        uint64_t count = 0;
        double_t avg = 0;
    };

    static unique_ptr<AggregateState> initialize() { return make_unique<AvgState>(); }

    static void update(uint8_t* state_, ValueVector* input, uint64_t multiplicity) {
        auto state = reinterpret_cast<AvgState*>(state_);
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
        AvgState* state, ValueVector* input, uint32_t pos, uint64_t multiplicity) {
        auto inputValues = (T*)input->values;
        for (auto i = 0u; i < multiplicity; ++i) {
            if (state->isNull) {
                state->sum = inputValues[pos];
                state->isNull = false;
            } else {
                Add::operation(state->sum, inputValues[pos], state->sum);
            }
        }
        state->count += multiplicity;
    }

    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto otherState = reinterpret_cast<AvgState*>(otherState_);
        if (otherState->isNull) {
            return;
        }
        auto state = reinterpret_cast<AvgState*>(state_);
        if (state->isNull) {
            state->sum = otherState->sum;
            state->isNull = false;
        } else {
            Add::operation(state->sum, otherState->sum, state->sum);
        }
        state->count = state->count + otherState->count;
    }

    static void finalize(uint8_t* state_) {
        auto state = reinterpret_cast<AvgState*>(state_);
        assert(!state->isNull);
        state->avg = state->sum / (double_t)state->count;
    }
};

template<>
inline void AvgFunction<Value>::finalize(uint8_t* state_) {
    auto state = reinterpret_cast<AvgState*>(state_);
    assert(!state->isNull);
    auto unstrCount = Value((double_t)state->count);
    auto result = Value();
    Divide::operation(state->sum, unstrCount, result);
    state->avg = result.val.doubleVal;
}

} // namespace function
} // namespace graphflow
