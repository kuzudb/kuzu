#pragma once

#include "src/common/include/clause_type.h"

using namespace graphflow::common;

namespace graphflow {
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
} // namespace graphflow
