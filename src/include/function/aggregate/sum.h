#pragma once

#include "function/aggregate_function.h"
#include "function/arithmetic/add.h"

namespace kuzu {
namespace function {

template<typename T>
struct SumFunction {

    struct SumState : public AggregateState {
        inline uint32_t getStateSize() const override { return sizeof(*this); }
        inline void moveResultToVector(common::ValueVector* outputVector, uint64_t pos) override {
            memcpy(outputVector->getData() + pos * outputVector->getNumBytesPerValue(),
                reinterpret_cast<uint8_t*>(&sum), outputVector->getNumBytesPerValue());
        }

        T sum{};
    };

    static std::unique_ptr<AggregateState> initialize() { return std::make_unique<SumState>(); }

    static void updateAll(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        storage::MemoryManager* /*memoryManager*/) {
        KU_ASSERT(!input->state->isFlat());
        auto* state = reinterpret_cast<SumState*>(state_);
        auto& inputSelVector = input->state->getSelVector();
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
                auto pos = inputSelVector[i];
                updateSingleValue(state, input, pos, multiplicity);
            }
        } else {
            for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
                auto pos = inputSelVector[i];
                if (!input->isNull(pos)) {
                    updateSingleValue(state, input, pos, multiplicity);
                }
            }
        }
    }

    static inline void updatePos(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        uint32_t pos, storage::MemoryManager* /*memoryManager*/) {
        auto* state = reinterpret_cast<SumState*>(state_);
        updateSingleValue(state, input, pos, multiplicity);
    }

    static void updateSingleValue(SumState* state, common::ValueVector* input, uint32_t pos,
        uint64_t multiplicity) {
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

    static void combine(uint8_t* state_, uint8_t* otherState_,
        storage::MemoryManager* /*memoryManager*/) {
        auto* otherState = reinterpret_cast<SumState*>(otherState_);
        if (otherState->isNull) {
            return;
        }
        auto* state = reinterpret_cast<SumState*>(state_);
        if (state->isNull) {
            state->sum = otherState->sum;
            state->isNull = false;
        } else {
            Add::operation(state->sum, otherState->sum, state->sum);
        }
    }

    static void finalize(uint8_t* /*state_*/) {}
};

} // namespace function
} // namespace kuzu
