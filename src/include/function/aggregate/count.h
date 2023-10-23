#pragma once

#include "base_count.h"

namespace kuzu {
namespace function {

struct CountFunction : public BaseCountFunction {

    static void updateAll(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        storage::MemoryManager* memoryManager);

    static inline void updatePos(uint8_t* state_, common::ValueVector* /*input*/,
        uint64_t multiplicity, uint32_t /*pos*/, storage::MemoryManager* /*memoryManager*/) {
        reinterpret_cast<CountState*>(state_)->count += multiplicity;
    }

    static void paramRewriteFunc(binder::expression_vector& arguments);
};

} // namespace function
} // namespace kuzu
