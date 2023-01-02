#pragma once

#include "aggregate_function.h"
#include "function/arithmetic/arithmetic_operations.h"

using namespace kuzu::function::operation;

namespace kuzu {
namespace function {

template<typename T>
struct SumFunction {

    struct SumState : public AggregateState {
        inline uint32_t getStateSize() const override { return sizeof(*this); }
        inline uint8_t* getResult() const override { return (uint8_t*)&sum; }

        T sum;
    };

    static unique_ptr<AggregateState> initialize() { return make_unique<SumState>(); }

    static void updateAll(uint8_t* state_, ValueVector* input, uint64_t multiplicity) {
        assert(!input->state->isFlat());
        auto state = reinterpret_cast<SumState*>(state_);
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < input->state->selVector->selectedSize; ++i) {
                auto pos = input->state->selVector->selectedPositions[i];
                updateSingleValue(state, input, pos, multiplicity);
            }
        } else {
            for (auto i = 0u; i < input->state->selVector->selectedSize; ++i) {
                auto pos = input->state->selVector->selectedPositions[i];
                if (!input->isNull(pos)) {
                    updateSingleValue(state, input, pos, multiplicity);
                }
            }
        }
    }

    static inline void updatePos(
        uint8_t* state_, ValueVector* input, uint64_t multiplicity, uint32_t pos) {
        auto state = reinterpret_cast<SumState*>(state_);
        updateSingleValue(state, input, pos, multiplicity);
    }

    static void updateSingleValue(
        SumState* state, ValueVector* input, uint32_t pos, uint64_t multiplicity) {
        T val = input->getValue<T>(pos);
        for (auto j = 0u; j < multiplicity; ++j) {
            if (state->isNull) {
                state->sum = val;
                state->isNull = false;
            } else {
                Add::operation(state->sum, val, state->sum);
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
            Add::operation(state->sum, otherState->sum, state->sum);
        }
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace kuzu
