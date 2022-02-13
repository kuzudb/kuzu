#pragma once

#include "return_clause.h"

namespace graphflow {
namespace parser {

/**
 * WIth statement may have where expression
 */
class WithClause : public ReturnClause {

public:
    WithClause(unique_ptr<ProjectionBody> projectionBody) : ReturnClause{move(projectionBody)} {}

    inline void setWhereExpression(unique_ptr<ParsedExpression> expression) {
        whereExpression = move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }

    inline ParsedExpression* getWhereExpression() const { return whereExpression.get(); }

    bool operator==(const WithClause& other) const;

private:
    unique_ptr<ParsedExpression> whereExpression;
};

} // namespace parser
} // namespace graphflow
