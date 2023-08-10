#pragma once

#include "binder/expression/rel_expression.h"
#include "update_table_type.h"

namespace kuzu {
namespace binder {

struct BoundCreateInfo {
    UpdateTableType updateTableType;
    std::shared_ptr<Expression> nodeOrRel;
    std::vector<expression_pair> setItems;

    BoundCreateInfo(UpdateTableType updateTableType, std::shared_ptr<Expression> nodeOrRel,
        std::vector<expression_pair> setItems)
        : updateTableType{updateTableType}, nodeOrRel{std::move(nodeOrRel)}, setItems{std::move(
                                                                                 setItems)} {}
    BoundCreateInfo(const BoundCreateInfo& other)
        : updateTableType{other.updateTableType}, nodeOrRel{other.nodeOrRel}, setItems{
                                                                                  other.setItems} {}

    inline std::unique_ptr<BoundCreateInfo> copy() {
        return std::make_unique<BoundCreateInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
