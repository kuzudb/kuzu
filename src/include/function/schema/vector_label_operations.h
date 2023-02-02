#pragma once

#include "function/vector_operations.h"
#include "label_operations.h"

namespace kuzu {
namespace function {

struct LabelVectorOperation : public VectorOperations {
    static void execFunction(const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::executeStringAndList<internalID_t, ku_list_t, ku_string_t,
            operation::Label>(*params[0], *params[1], result);
    }
};

} // namespace function
} // namespace kuzu
