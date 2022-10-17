#pragma once

#include <memory>

#include "src/binder/expression/include/expression.h"
#include "src/common/include/clause_type.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace binder {

class BoundReadingClause {

public:
    explicit BoundReadingClause(ClauseType clauseType) : clauseType{clauseType} {}
    virtual ~BoundReadingClause() = default;

    ClauseType getClauseType() const { return clauseType; }

    inline virtual expression_vector getSubPropertyExpressions() const = 0;

    inline virtual unique_ptr<BoundReadingClause> copy() = 0;

private:
    ClauseType clauseType;
};
} // namespace binder
} // namespace graphflow
