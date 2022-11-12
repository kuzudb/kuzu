#pragma once

#include <functional>

#include "src/common/include/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ConstOperationExecutor {

    template<typename RESULT_TYPE, typename OP>
    static void execute(ValueVector& result) {
        assert(result.state->isFlat());
        auto resultValues = (RESULT_TYPE*)result.values;
        auto idx = result.state->getPositionOfCurrIdx();
        assert(idx == 0);
        OP::operation(resultValues[idx]);
    }
};

} // namespace function
} // namespace kuzu
