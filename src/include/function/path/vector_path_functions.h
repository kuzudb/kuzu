#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct NodesFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct RelsFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct PropertiesBindData : public FunctionBindData {
    common::vector_idx_t childIdx;

    PropertiesBindData(common::LogicalType dataType, common::vector_idx_t childIdx)
        : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}
};

struct PropertiesFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
    static void compileFunc(FunctionBindData* bindData,
        const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        std::shared_ptr<common::ValueVector>& result);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr);
};

struct IsTrailFunction {
    static function_set getFunctionSet();
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr);
    static bool selectFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::SelectionVector& selectionVector);
};

struct IsACyclicFunction {
    static function_set getFunctionSet();
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr);
    static bool selectFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::SelectionVector& selectionVector);
};

} // namespace function
} // namespace kuzu
