#pragma once

#include "src/function/include/aggregate/aggregate_function.h"

namespace graphflow {
namespace function {

struct BaseCountFunction {

    struct CountState : public AggregateState {
        uint64_t val = 0;

        inline uint64_t getValSize() const override { return sizeof(*this); }
        inline uint8_t* getFinalVal() const override { return (uint8_t*)&val; }
    };

    static unique_ptr<AggregateState> initialize() {
        auto state = make_unique<CountState>();
        state->isNull = false;
        return state;
    }

    static void combine(uint8_t* state_, uint8_t* otherState_) {
        auto state = reinterpret_cast<CountState*>(state_);
        auto otherState = reinterpret_cast<CountState*>(otherState_);
        state->val += otherState->val;
    }

    static void finalize(uint8_t* state_) {}
};

} // namespace function
} // namespace graphflow
