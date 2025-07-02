#pragma once

#include "common/types/types.h"
#include "function/function.h"

#include <iostream>

namespace kuzu {
namespace function {
struct ScalarFunction;

struct CastToUnionBindData : public FunctionBindData {
    common::union_field_idx_t minCostTag;
    std::shared_ptr<ScalarFunction> innerCast;
    common::LogicalType innerType;

    CastToUnionBindData(common::union_field_idx_t minCostTag, std::shared_ptr<ScalarFunction> innerCast, common::LogicalType innerType, common::LogicalType dataType) : FunctionBindData{std::move(dataType)}, minCostTag{minCostTag}, innerCast{innerCast}, innerType{std::move(innerType)} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<CastToUnionBindData>(minCostTag, innerCast, innerType.copy(), resultType.copy());
    }   
};

} // namespace function
} // namespace kuzu
