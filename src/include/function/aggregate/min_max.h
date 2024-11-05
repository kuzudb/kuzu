#pragma once

#include "function/aggregate_function.h"

namespace kuzu {
namespace function {

template<typename T>
struct MinMaxFunction {

    struct MinMaxState : public AggregateState {
        uint32_t getStateSize() const override { return sizeof(*this); }
        void moveResultToVector(common::ValueVector* outputVector, uint64_t pos) override {
            outputVector->setValue(pos, val);
            overflowBuffer.reset();
        }
        void setVal(T& val_, storage::MemoryManager* /*memoryManager*/) { val = val_; }

        std::unique_ptr<common::InMemOverflowBuffer> overflowBuffer;
        T val{};
    };

    static std::unique_ptr<AggregateState> initialize() { return std::make_unique<MinMaxState>(); }

    template<class OP>
    static void updateAll(uint8_t* state_, common::ValueVector* input, uint64_t /*multiplicity*/,
        storage::MemoryManager* memoryManager) {
        KU_ASSERT(!input->state->isFlat());
        auto* state = reinterpret_cast<MinMaxState*>(state_);
        auto& inputSelVector = input->state->getSelVector();
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
                auto pos = inputSelVector[i];
                updateSingleValue<OP>(state, input, pos, memoryManager);
            }
        } else {
            for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
                auto pos = inputSelVector[i];
                if (!input->isNull(pos)) {
                    updateSingleValue<OP>(state, input, pos, memoryManager);
                }
            }
        }
    }

    template<class OP>
    static inline void updatePos(uint8_t* state_, common::ValueVector* input,
        uint64_t /*multiplicity*/, uint32_t pos, storage::MemoryManager* memoryManager) {
        updateSingleValue<OP>(reinterpret_cast<MinMaxState*>(state_), input, pos, memoryManager);
    }

    template<class OP>
    static void updateSingleValue(MinMaxState* state, common::ValueVector* input, uint32_t pos,
        storage::MemoryManager* memoryManager) {
        T val = input->getValue<T>(pos);
        if (state->isNull) {
            state->setVal(val, memoryManager);
            state->isNull = false;
        } else {
            uint8_t compare_result = 0;
            OP::template operation<T, T>(val, state->val, compare_result, nullptr /* leftVector */,
                nullptr /* rightVector */);
            if (compare_result) {
                state->setVal(val, memoryManager);
            }
        }
    }

    template<class OP>
    static void combine(uint8_t* state_, uint8_t* otherState_,
        storage::MemoryManager* memoryManager) {
        auto* otherState = reinterpret_cast<MinMaxState*>(otherState_);
        if (otherState->isNull) {
            return;
        }
        auto* state = reinterpret_cast<MinMaxState*>(state_);
        if (state->isNull) {
            state->setVal(otherState->val, memoryManager);
            state->isNull = false;
        } else {
            uint8_t compareResult = 0;
            OP::template operation<T, T>(otherState->val, state->val, compareResult,
                nullptr /* leftVector */, nullptr /* rightVector */);
            if (compareResult) {
                state->setVal(otherState->val, memoryManager);
            }
        }
        otherState->overflowBuffer.reset();
    }

    static void finalize(uint8_t* /*state_*/) {}
};

template<>
void MinMaxFunction<common::ku_string_t>::MinMaxState::setVal(common::ku_string_t& val_,
    storage::MemoryManager* memoryManager) {
    if (overflowBuffer == nullptr) {
        overflowBuffer = std::make_unique<common::InMemOverflowBuffer>(memoryManager);
    }
    // We only need to allocate memory if the new val_ is a long string and is longer
    // than the current val.
    if (val_.len > common::ku_string_t::SHORT_STR_LENGTH && val_.len > val.len) {
        val.overflowPtr = reinterpret_cast<uint64_t>(overflowBuffer->allocateSpace(val_.len));
    }
    val.set(val_);
}

} // namespace function
} // namespace kuzu
