#pragma once

#include "common/types/types.h"
#include "function/cast/cast_function_bind_data.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct CastToUnionBindData : public CastFunctionBindData {
    using inner_func_t =
        std::function<void(common::ValueVector&, common::ValueVector&, common::SelectionVector&)>;

    common::union_field_idx_t targetTag;
    inner_func_t innerFunc;

    CastToUnionBindData(common::union_field_idx_t targetTag, inner_func_t innerFunc,
        common::LogicalType dataType)
        : CastFunctionBindData{std::move(dataType)}, targetTag{targetTag},
          innerFunc{std::move(innerFunc)} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<CastToUnionBindData>(targetTag, innerFunc, resultType.copy());
    }
};

} // namespace function
} // namespace kuzu
