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
        : deleteClauseType{deleteClauseType}, updateTableType{updateTableType}, nodeOrRel{std::move(
                                                                                    nodeOrRel)} {}
    BoundDeleteInfo(const BoundDeleteInfo& other)
        : deleteClauseType{other.deleteClauseType},
          updateTableType{other.updateTableType}, nodeOrRel{other.nodeOrRel} {}

    inline std::unique_ptr<BoundDeleteInfo> copy() {
        return std::make_unique<BoundDeleteInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
