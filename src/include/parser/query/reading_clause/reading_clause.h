#pragma once

#include "common/enums/clause_type.h"

namespace kuzu {
namespace parser {

class ReadingClause {
public:
    explicit ReadingClause(common::ClauseType clauseType) : clauseType{clauseType} {};
    virtual ~ReadingClause() = default;

    inline common::ClauseType getClauseType() const { return clauseType; }

private:
    common::ClauseType clauseType;
};
} // namespace parser
} // namespace kuzu
