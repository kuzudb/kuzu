#pragma once

#include "common/vector/value_vector_utils.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct UnionValueVectorOperations : public VectorOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
};

struct UnionTagVectorOperations : public VectorOperations {
    static vector_operation_definitions getDefinitions();
};

struct UnionExtractVectorOperations : public VectorOperations {
    static vector_operation_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
