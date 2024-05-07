#pragma once

#include "common/types/int128_t.h"
#include "function/aggregate_function.h"
#include "function/arithmetic/add.h"

namespace kuzu {
namespace function {

template<typename T>
struct AvgFunction {

    struct AvgState : public AggregateState {
        inline uint32_t getStateSize() const override { return sizeof(*this); }
        inline void moveResultToVector(common::ValueVector* outputVector, uint64_t pos) override {
            memcpy(outputVector->getData() + pos * outputVector->getNumBytesPerValue(),
                reinterpret_cast<uint8_t*>(&avg), outputVector->getNumBytesPerValue());
        }

        T sum;
        uint64_t count = 0;
        double avg = 0;
    };

    static std::unique_ptr<AggregateState> initialize() { return std::make_unique<AvgState>(); }

    static void updateAll(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        storage::MemoryManager* /*memoryManager*/) {
        auto* state = reinterpret_cast<AvgState*>(state_);
        KU_ASSERT(!input->state->isFlat());
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < input->state->getSelVector().getSelSize(); ++i) {
                auto pos = input->state->getSelVector()[i];
                updateSingleValue(state, input, pos, multiplicity);
            }
        } else {
            for (auto i = 0u; i < input->state->getSelVector().getSelSize(); ++i) {
                auto pos = input->state->getSelVector()[i];
                if (!input->isNull(pos)) {
                    updateSingleValue(state, input, pos, multiplicity);
                }
            }
        }
    }

    static inline void updatePos(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        uint32_t pos, storage::MemoryManager* /*memoryManager*/) {
        updateSingleValue(reinterpret_cast<AvgState*>(state_), input, pos, multiplicity);
    }

    static void updateSingleValue(AvgState* state, common::ValueVector* input, uint32_t pos,
        uint64_t multiplicity) {
        T val = input->getValue<T>(pos);
        for (auto i = 0u; i < multiplicity; ++i) {
            if (state->isNull) {
                state->sum = val;
                state->isNull = false;
            } else {
                Add::operation(state->sum, val, state->sum);
            }
        }
        state->count += multiplicity;
    }

    static void combine(uint8_t* state_, uint8_t* otherState_,
        storage::MemoryManager* /*memoryManager*/) {
        auto* otherState = reinterpret_cast<AvgState*>(otherState_);
        if (otherState->isNull) {
            return;
        }
        auto* state = reinterpret_cast<AvgState*>(state_);
        if (state->isNull) {
            state->sum = otherState->sum;
            state->isNull = false;
        } else {
            Add::operation(state->sum, otherState->sum, state->sum);
        }
        state->count = state->count + otherState->count;
    }

    static void finalize(uint8_t* state_) {
        auto* state = reinterpret_cast<AvgState*>(state_);
        if (!state->isNull) {
            state->avg = state->sum / (double)state->count;
        }
    }
};

template<>
void AvgFunction<common::int128_t>::finalize(uint8_t* state_) {
    auto state = reinterpret_cast<AvgState*>(state_);
    if (!state->isNull) {
        state->avg = common::Int128_t::Cast<long double>(state->sum) /
                     common::Int128_t::Cast<long double>(state->count);
    }
}

} // namespace function
} // namespace kuzu
