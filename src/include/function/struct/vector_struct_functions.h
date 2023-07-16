#pragma once

#include "common/vector/value_vector.h"
#include "function/vector_functions.h"

namespace kuzu {
namespace function {

struct StructPackVectorFunctions {
    static vector_function_definitions getDefinitions();

    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
    static void compileFunc(FunctionBindData* bindData,
        const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        std::shared_ptr<common::ValueVector>& result);
    static void copyParameterValueToStructFieldVector(const common::ValueVector* parameter,
        common::ValueVector* structField, common::DataChunkState* structVectorState);
};

struct StructExtractBindData : public FunctionBindData {
    common::vector_idx_t childIdx;

    StructExtractBindData(common::LogicalType dataType, common::vector_idx_t childIdx)
        : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}
};

struct StructExtractVectorFunctions {
    static vector_function_definitions getDefinitions();

    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void compileFunc(FunctionBindData* bindData,
        const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        std::shared_ptr<common::ValueVector>& result);
};

} // namespace function
} // namespace kuzu
