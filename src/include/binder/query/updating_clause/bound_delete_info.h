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
    EXPLICIT_COPY_DEFAULT_MOVE(BoundDeleteInfo);

private:
    BoundDeleteInfo(const BoundDeleteInfo& other)
        : deleteClauseType{other.deleteClauseType},
          updateTableType{other.updateTableType}, nodeOrRel{other.nodeOrRel} {}
};

} // namespace binder
} // namespace kuzu
