#pragma once

#include "base_count.h"

namespace kuzu {
namespace function {

struct CountFunction : public BaseCountFunction {
    static constexpr const char* name = "COUNT";

    static void updateAll(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        storage::MemoryManager* memoryManager);

    // NOLINTNEXTLINE(readability-non-const-parameter): Would cast away qualifiers.
    static inline void updatePos(uint8_t* state_, common::ValueVector* /*input*/,
        uint64_t multiplicity, uint32_t /*pos*/, storage::MemoryManager* /*memoryManager*/) {
        reinterpret_cast<CountState*>(state_)->count += multiplicity;
    }

    static void paramRewriteFunc(binder::expression_vector& arguments);

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
