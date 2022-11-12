#pragma once

#include "aggregate_function.h"

namespace kuzu {
namespace function {

struct BaseCountFunction {

    struct CountState : public AggregateState {
        inline uint32_t getStateSize() const override { return sizeof(*this); }
        inline uint8_t* getResult() const override { return (uint8_t*)&count; }

        uint64_t count = 0;
    };

    static unique_ptr<AggregateState> initialize() {
        auto state = make_unique<CountState>();
        state->isNull = false;
        return state;
    }

    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto state = reinterpret_cast<CountState*>(state_);
        auto otherState = reinterpret_cast<CountState*>(otherState_);
        state->count += otherState->count;
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace kuzu
