#pragma once

#include "function/vector_operations.h"
#include "offset_operations.h"

namespace kuzu {
namespace function {

struct OffsetVectorOperation {
    static void execFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::execute<common::internalID_t, int64_t, operation::Offset>(
            *params[0], result);
    }

    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
        definitions.push_back(make_unique<VectorOperationDefinition>(common::OFFSET_FUNC_NAME,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::INTERNAL_ID},
            common::LogicalTypeID::INT64, OffsetVectorOperation::execFunction));
        return definitions;
    }
};

} // namespace function
} // namespace kuzu
