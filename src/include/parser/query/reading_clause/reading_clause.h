#pragma once

#include "common/clause_type.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

class ReadingClause {
public:
    explicit ReadingClause(ClauseType clauseType) : clauseType{clauseType} {};
    virtual ~ReadingClause() = default;

    inline ClauseType getClauseType() const { return clauseType; }

private:
    ClauseType clauseType;
};
} // namespace parser
} // namespace kuzu
