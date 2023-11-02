#pragma once

#include <memory>

#include "common/enums/clause_type.h"

namespace kuzu {
namespace binder {

class BoundReadingClause {
public:
    explicit BoundReadingClause(common::ClauseType clauseType) : clauseType{clauseType} {}
    virtual ~BoundReadingClause() = default;

    common::ClauseType getClauseType() const { return clauseType; }

    inline virtual std::unique_ptr<BoundReadingClause> copy() = 0;

private:
    common::ClauseType clauseType;
};
} // namespace binder
} // namespace kuzu
