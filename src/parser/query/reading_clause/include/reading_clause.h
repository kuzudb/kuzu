#pragma once

#include "src/common/include/clause_type.h"

using namespace graphflow::common;

namespace graphflow {
namespace parser {

class ReadingClause {
public:
    explicit ReadingClause(ClauseType clauseType) : clauseType{clauseType} {};
    virtual ~ReadingClause() = default;

    inline ClauseType getClauseType() const { return clauseType; }

    virtual bool equals(const ReadingClause& other) const { return clauseType == other.clauseType; }

    bool operator==(const ReadingClause& other) const { return equals(other); }

    bool operator!=(const ReadingClause& other) const { return equals(other); }

private:
    ClauseType clauseType;
};
} // namespace parser
} // namespace graphflow
