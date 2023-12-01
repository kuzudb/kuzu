#pragma once

#include "function/binary_function_executor.h"
#include "function/schema/label_functions.h"

namespace kuzu {
namespace function {

struct LabelFunction {
    static void execFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeListExtract<common::internalID_t, common::list_entry_t,
            common::ku_string_t, Label>(*params[0], *params[1], result);
    }
};

} // namespace function
} // namespace kuzu
