#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct StructPackVectorOperations : public VectorOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result) {} // Evaluate at compile time
};

struct StructExtractBindData : public FunctionBindData {
    common::vector_idx_t childIdx;

    StructExtractBindData(common::DataType dataType, common::vector_idx_t childIdx)
        : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}
};

struct StructExtractVectorOperations : public VectorOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result) {} // Evaluate at compile time
};

} // namespace function
} // namespace kuzu
