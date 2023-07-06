#pragma once

#include <functional>

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ConstFunctionExecutor {

    template<typename RESULT_TYPE, typename OP>
    static void execute(common::ValueVector& result) {
        assert(result.state->isFlat());
        auto resultValues = (RESULT_TYPE*)result.getData();
        auto idx = result.state->selVector->selectedPositions[0];
        assert(idx == 0);
        OP::operation(resultValues[idx]);
    }
};

} // namespace function
} // namespace kuzu
