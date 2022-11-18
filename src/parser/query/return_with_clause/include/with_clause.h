#pragma once

#include "return_clause.h"

namespace kuzu {
namespace parser {

/**
 * WIth clause may have where expression
 */
class WithClause : public ReturnClause {

public:
    WithClause(unique_ptr<ProjectionBody> projectionBody) : ReturnClause{move(projectionBody)} {}

    inline void setWhereExpression(unique_ptr<ParsedExpression> expression) {
        whereExpression = move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }

    inline ParsedExpression* getWhereExpression() const { return whereExpression.get(); }

private:
    unique_ptr<ParsedExpression> whereExpression;
};

} // namespace parser
} // namespace kuzu
