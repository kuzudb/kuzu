#pragma once

#include "common/types/types.h"
#include "function/cast/cast_function_bind_data.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct CastToUnionBindData : public CastFunctionBindData {
    common::union_field_idx_t targetTag;
    CastFunctionBindData innerBindData;

    CastToUnionBindData(common::union_field_idx_t targetTag, common::LogicalType innerType,
        common::LogicalType dataType)
        : CastFunctionBindData{std::move(dataType)}, targetTag{targetTag},
          innerBindData{CastFunctionBindData(std::move(innerType))} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<CastToUnionBindData>(targetTag, innerBindData.resultType.copy(),
            resultType.copy());
    }
};

struct CastBetweenUnionBindData : public CastFunctionBindData {
    std::shared_ptr<std::vector<std::unique_ptr<CastToUnionBindData>>> innerCasts;

    CastBetweenUnionBindData(
        const std::shared_ptr<std::vector<std::unique_ptr<CastToUnionBindData>>>& innerCasts,
        common::LogicalType dataType)
        : CastFunctionBindData{std::move(dataType)}, innerCasts{innerCasts} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<CastBetweenUnionBindData>(innerCasts, resultType.copy());
    }
};

} // namespace function
} // namespace kuzu
