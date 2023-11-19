#pragma once

#include "binder/expression/expression.h"
#include "common/enums/clause_type.h"
#include "update_table_type.h"

namespace kuzu {
namespace binder {

struct BoundDeleteInfo {
    common::DeleteClauseType deleteClauseType;
    UpdateTableType updateTableType;
    std::shared_ptr<Expression> nodeOrRel;

    BoundDeleteInfo(UpdateTableType updateTableType, std::shared_ptr<Expression> nodeOrRel,
        common::DeleteClauseType deleteClauseType)
        : updateTableType{updateTableType}, nodeOrRel{std::move(nodeOrRel)},
          deleteClauseType{deleteClauseType} {}
    BoundDeleteInfo(const BoundDeleteInfo& other)
        : updateTableType{other.updateTableType}, nodeOrRel{other.nodeOrRel},
          deleteClauseType{other.deleteClauseType} {}

    inline std::unique_ptr<BoundDeleteInfo> copy() {
        return std::make_unique<BoundDeleteInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
