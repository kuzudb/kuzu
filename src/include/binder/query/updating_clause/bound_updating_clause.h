#pragma once

#include "common/cast.h"
#include "common/enums/clause_type.h"

namespace kuzu {
namespace binder {

class BoundUpdatingClause {
public:
    explicit BoundUpdatingClause(common::ClauseType clauseType) : clauseType{clauseType} {}
    virtual ~BoundUpdatingClause() = default;

    common::ClauseType getClauseType() const { return clauseType; }

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const BoundUpdatingClause&, const TARGET&>(*this);
    }

private:
    common::ClauseType clauseType;
};

} // namespace binder
} // namespace kuzu
