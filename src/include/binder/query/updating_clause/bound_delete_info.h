#pragma once

#include "binder/expression/expression.h"
#include "update_table_type.h"

namespace kuzu {
namespace binder {

struct BoundDeleteInfo {
    UpdateTableType updateTableType;
    std::shared_ptr<Expression> nodeOrRel;

    BoundDeleteInfo(UpdateTableType updateTableType, std::shared_ptr<Expression> nodeOrRel)
        : updateTableType{updateTableType}, nodeOrRel{std::move(nodeOrRel)} {}
    BoundDeleteInfo(const BoundDeleteInfo& other)
        : updateTableType{other.updateTableType}, nodeOrRel{other.nodeOrRel} {}

    inline std::unique_ptr<BoundDeleteInfo> copy() {
        return std::make_unique<BoundDeleteInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
