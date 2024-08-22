#pragma once

#include "function/binary_function_executor.h"
#include "function/schema/keys_function.h"

namespace kuzu {
namespace function {

static void operation(common::internalID_t& left, common::list_entry_t& right,
    common::ku_string_t& result, common::ValueVector& leftVector, common::ValueVector& rightVector,
    common::ValueVector& resultVector, uint64_t resPos) {
    KU_ASSERT(left.tableID < right.size);
    ListExtract::operation(right, left.tableID + 1 /* listExtract requires 1-based index */, result,
        rightVector, leftVector, resultVector, resPos);
}

void KeysFunction::execFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
    common::ValueVector& result, void*) {
    KU_ASSERT(params.size() == 2);
    BinaryFunctionExecutor::executeListExtract<common::internalID_t, common::list_entry_t,
        common::ku_string_t, Label>(*params[0], *params[1], result);
}

} // namespace function
} // namespace kuzu
