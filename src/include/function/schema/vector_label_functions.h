#pragma once

#include "function/vector_functions.h"
#include "label_functions.h"

namespace kuzu {
namespace function {

struct LabelVectorFunction {
    static void execFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 2);
        BinaryFunctionExecutor::executeListStruct<common::internalID_t, common::list_entry_t,
            common::ku_string_t, Label>(*params[0], *params[1], result);
    }
};

} // namespace function
} // namespace kuzu
