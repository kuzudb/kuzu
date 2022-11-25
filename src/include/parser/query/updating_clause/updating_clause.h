#pragma once

#include "common/clause_type.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

class UpdatingClause {

public:
    explicit UpdatingClause(ClauseType clauseType) : clauseType{clauseType} {};
    virtual ~UpdatingClause() = default;

    ClauseType getClauseType() const { return clauseType; }

private:
    ClauseType clauseType;
};

} // namespace parser
} // namespace kuzu
