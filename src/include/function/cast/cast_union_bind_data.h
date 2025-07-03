#pragma once

#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"
#include "function/cast/cast_function_bind_data.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct CastToUnionBindData : public FunctionBindData {
    using inner_func_t = std::function<void(common::ValueVector&, common::ValueVector&,
        common::SelectionVector&, CastToUnionBindData&)>;

    common::union_field_idx_t minCostTag;
    inner_func_t innerFunc;
    CastFunctionBindData innerBindData;

    CastToUnionBindData(common::union_field_idx_t minCostTag, inner_func_t innerFunc,
        common::LogicalType innerType, common::LogicalType dataType)
        : FunctionBindData{std::move(dataType)}, minCostTag{minCostTag},
          innerFunc{std::move(innerFunc)},
          innerBindData{CastFunctionBindData(std::move(innerType))} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<CastToUnionBindData>(minCostTag, innerFunc,
            innerBindData.resultType.copy(), resultType.copy());
    }
};

} // namespace function
} // namespace kuzu
