#pragma once

#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"
#include "function/cast/cast_function_bind_data.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct CastToUnionBindData : public FunctionBindData {
    using inner_func_t = std::function<void(common::ValueVector&, common::ValueVector&,
        common::SelectionVector&, CastFunctionBindData&)>;

    common::union_field_idx_t targetTag;
    inner_func_t innerFunc;
    CastFunctionBindData innerBindData;

    CastToUnionBindData(common::union_field_idx_t targetTag, inner_func_t innerFunc,
        common::LogicalType innerType, common::LogicalType dataType)
        : FunctionBindData{std::move(dataType)}, targetTag{targetTag},
          innerFunc{std::move(innerFunc)},
          innerBindData{CastFunctionBindData(std::move(innerType))} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<CastToUnionBindData>(targetTag, innerFunc,
            innerBindData.resultType.copy(), resultType.copy());
    }
};

struct CastBetweenUnionBindData : public FunctionBindData {
    std::shared_ptr<std::vector<std::unique_ptr<CastToUnionBindData>>> innerCasts;

    CastBetweenUnionBindData(const std::shared_ptr<std::vector<std::unique_ptr<CastToUnionBindData>>>& innerCasts, common::LogicalType dataType)
        : FunctionBindData{std::move(dataType)}, innerCasts{std::move(innerCasts)} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<CastBetweenUnionBindData>(innerCasts, resultType.copy());
    }
};

} // namespace function
} // namespace kuzu
