#pragma once

#include "binder/expression/expression.h"
#include "common/clause_type.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

class BoundUpdatingClause {

public:
    explicit BoundUpdatingClause(ClauseType clauseType) : clauseType{clauseType} {}
    virtual ~BoundUpdatingClause() = default;

    ClauseType getClauseType() const { return clauseType; }

    virtual expression_vector getPropertiesToRead() const = 0;

    virtual unique_ptr<BoundUpdatingClause> copy() = 0;

private:
    ClauseType clauseType;
};

} // namespace binder
} // namespace kuzu
