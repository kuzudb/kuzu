#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct PointerFunctionExecutor {
    template<typename RESULT_TYPE, typename OP>
    static void execute(common::ValueVector& result, void* dataPtr) {
        if (result.state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
                OP::operation(result.getValue<RESULT_TYPE>(i), dataPtr);
            }
        } else {
            for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
                auto pos = result.state->selVector->selectedPositions[i];
                OP::operation(result.getValue<RESULT_TYPE>(pos), dataPtr);
            }
        }
    }
};

} // namespace function
} // namespace kuzu
