#pragma once

#include "common/types/types.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct CastToUnionBindData : public FunctionBindData {
    common::union_field_idx_t minCostTag;
    CastToUnionBindData(common::union_field_idx_t minCostTag, common::LogicalType dataType)
        : FunctionBindData{std::move(dataType)}, minCostTag{minCostTag} {}
};

} // namespace function
} // namespace kuzu
