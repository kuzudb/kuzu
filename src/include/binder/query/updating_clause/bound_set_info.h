#pragma once

#include "binder/expression/expression.h"
#include "update_table_type.h"

namespace kuzu {
namespace binder {

struct BoundSetPropertyInfo {
    UpdateTableType updateTableType;
    std::shared_ptr<Expression> nodeOrRel;
    expression_pair setItem;

    BoundSetPropertyInfo(UpdateTableType updateTableType, std::shared_ptr<Expression> nodeOrRel,
        expression_pair setItem)
        : updateTableType{updateTableType}, nodeOrRel{std::move(nodeOrRel)}, setItem{std::move(
                                                                                 setItem)} {}
    BoundSetPropertyInfo(const BoundSetPropertyInfo& other)
        : updateTableType{other.updateTableType}, nodeOrRel{other.nodeOrRel}, setItem{
                                                                                  other.setItem} {}

    inline std::unique_ptr<BoundSetPropertyInfo> copy() const {
        return std::make_unique<BoundSetPropertyInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
