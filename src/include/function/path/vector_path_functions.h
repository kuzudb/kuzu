#pragma once

#include "function/vector_functions.h"

namespace kuzu {
namespace function {

struct NodesVectorFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct RelsVectorFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct PropertiesBindData : public FunctionBindData {
    common::vector_idx_t childIdx;

    PropertiesBindData(common::LogicalType dataType, common::vector_idx_t childIdx)
        : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}
};

struct PropertiesVectorFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void compileFunc(FunctionBindData* bindData,
        const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        std::shared_ptr<common::ValueVector>& result);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
};

struct IsTrailVectorFunction {
    static vector_function_definitions getDefinitions();
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
    static bool selectFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::SelectionVector& selectionVector);
};

struct IsACyclicVectorFunction {
    static vector_function_definitions getDefinitions();
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
    static bool selectFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::SelectionVector& selectionVector);
};

} // namespace function
} // namespace kuzu
