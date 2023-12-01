#pragma once

#include "binder/expression/expression.h"
#include "update_table_type.h"

namespace kuzu {
namespace binder {

struct BoundInsertInfo {
    UpdateTableType updateTableType;
    std::shared_ptr<Expression> nodeOrRel;
    std::vector<expression_pair> setItems;

    BoundInsertInfo(UpdateTableType updateTableType, std::shared_ptr<Expression> nodeOrRel,
        std::vector<expression_pair> setItems)
        : updateTableType{updateTableType}, nodeOrRel{std::move(nodeOrRel)}, setItems{std::move(
                                                                                 setItems)} {}
    BoundInsertInfo(const BoundInsertInfo& other)
        : updateTableType{other.updateTableType}, nodeOrRel{other.nodeOrRel}, setItems{
                                                                                  other.setItems} {}

    inline std::unique_ptr<BoundInsertInfo> copy() {
        return std::make_unique<BoundInsertInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
