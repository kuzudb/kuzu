#pragma once

#include "function/scalar_function.h"
#include "offset_functions.h"

namespace kuzu {
namespace function {

struct OffsetFunction {
    static function_set getFunctionSet();
    static void execFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::execute<common::internalID_t, int64_t, Offset>(*params[0], result);
    }
};

} // namespace function
} // namespace kuzu
