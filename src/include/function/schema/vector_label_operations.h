#pragma once

#include "function/vector_operations.h"
#include "label_operations.h"

namespace kuzu {
namespace function {

struct LabelVectorOperation {
    static void execFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::executeListStruct<common::internalID_t, common::list_entry_t,
            common::ku_string_t, operation::Label>(*params[0], *params[1], result);
    }
};

} // namespace function
} // namespace kuzu
