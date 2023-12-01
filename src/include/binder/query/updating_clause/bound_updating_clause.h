#pragma once

#include <memory>

#include "common/enums/clause_type.h"

namespace kuzu {
namespace binder {

class BoundUpdatingClause {

public:
    explicit BoundUpdatingClause(common::ClauseType clauseType) : clauseType{clauseType} {}
    virtual ~BoundUpdatingClause() = default;

    common::ClauseType getClauseType() const { return clauseType; }

    virtual std::unique_ptr<BoundUpdatingClause> copy() = 0;

private:
    common::ClauseType clauseType;
};

} // namespace binder
} // namespace kuzu
