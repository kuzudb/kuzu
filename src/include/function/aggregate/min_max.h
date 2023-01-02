#pragma once

#include "aggregate_function.h"
#include "function/comparison/comparison_operations.h"

using namespace kuzu::function::operation;

namespace kuzu {
namespace function {

template<typename T>
struct MinMaxFunction {

    struct MinMaxState : public AggregateState {
        inline uint32_t getStateSize() const override { return sizeof(*this); }
        inline uint8_t* getResult() const override { return (uint8_t*)&val; }

        T val;
    };

    static unique_ptr<AggregateState> initialize() { return make_unique<MinMaxState>(); }

    template<class OP>
    static void updateAll(uint8_t* state_, ValueVector* input, uint64_t multiplicity) {
        assert(!input->state->isFlat());
        auto state = reinterpret_cast<MinMaxState*>(state_);
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < input->state->selVector->selectedSize; ++i) {
                auto pos = input->state->selVector->selectedPositions[i];
                updateSingleValue<OP>(state, input, pos);
            }
        } else {
            for (auto i = 0u; i < input->state->selVector->selectedSize; ++i) {
                auto pos = input->state->selVector->selectedPositions[i];
                if (!input->isNull(pos)) {
                    updateSingleValue<OP>(state, input, pos);
                }
            }
        }
    }

    template<class OP>
    static inline void updatePos(
        uint8_t* state_, ValueVector* input, uint64_t multiplicity, uint32_t pos) {
        updateSingleValue<OP>(reinterpret_cast<MinMaxState*>(state_), input, pos);
    }

    template<class OP>
    static void updateSingleValue(MinMaxState* state, ValueVector* input, uint32_t pos) {
        T val = input->getValue<T>(pos);
        if (state->isNull) {
            state->val = val;
            state->isNull = false;
        } else {
            uint8_t compare_result;
            OP::template operation<T, T>(val, state->val, compare_result);
            state->val = compare_result ? val : state->val;
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
} // namespace kuzu
