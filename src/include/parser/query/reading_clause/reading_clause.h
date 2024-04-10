#pragma once

#include "common/cast.h"
#include "common/enums/clause_type.h"

namespace kuzu {
namespace parser {

class ReadingClause {
public:
    explicit ReadingClause(common::ClauseType clauseType) : clauseType{clauseType} {};
    virtual ~ReadingClause() = default;

    common::ClauseType getClauseType() const { return clauseType; }

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const ReadingClause&, const TARGET&>(*this);
    }

private:
    common::ClauseType clauseType;
};
} // namespace parser
} // namespace kuzu
