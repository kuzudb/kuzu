#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace function {

struct PointerFunctionExecutor {

    template<typename RESULT_TYPE, typename OP>
    static void execute(common::ValueVector& result, void* dataPtr) {
        OP::operation(result, dataPtr);
    }
};

} // namespace function
} // namespace kuzu
