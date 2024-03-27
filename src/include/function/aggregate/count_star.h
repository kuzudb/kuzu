#pragma once

#include "base_count.h"

namespace kuzu {
namespace function {

struct CountStarFunction : public BaseCountFunction {
    static constexpr const char* name = "COUNT_STAR";

    static void updateAll(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        storage::MemoryManager* /*memoryManager*/);

    static void updatePos(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        uint32_t /*pos*/, storage::MemoryManager* /*memoryManager*/);

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
