#pragma once

#include "common/vector/value_vector.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct StructPackFunctions {
    static constexpr const char* name = "STRUCT_PACK";

    static function_set getFunctionSet();

    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr);
    static void compileFunc(FunctionBindData* bindData,
        const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        std::shared_ptr<common::ValueVector>& result);
};

struct StructExtractBindData : public FunctionBindData {
    common::vector_idx_t childIdx;

    StructExtractBindData(std::unique_ptr<common::LogicalType> dataType,
        common::vector_idx_t childIdx)
        : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}
};

struct StructExtractFunctions {
    static constexpr const char* name = "STRUCT_EXTRACT";

    static function_set getFunctionSet();

    static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
        Function* function);
    static void compileFunc(FunctionBindData* bindData,
        const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        std::shared_ptr<common::ValueVector>& result);
};

} // namespace function
} // namespace kuzu
