#pragma once

#include "common/enums/clause_type.h"

namespace kuzu {
namespace parser {

class UpdatingClause {

public:
    explicit UpdatingClause(common::ClauseType clauseType) : clauseType{clauseType} {};
    virtual ~UpdatingClause() = default;

    common::ClauseType getClauseType() const { return clauseType; }

private:
    common::ClauseType clauseType;
};

} // namespace parser
} // namespace kuzu
