#pragma once

#include "return_clause.h"

namespace kuzu {
namespace parser {

/**
 * WIth clause may have where expression
 */
class WithClause : public ReturnClause {

public:
    explicit WithClause(std::unique_ptr<ProjectionBody> projectionBody)
        : ReturnClause{std::move(projectionBody)} {}

    inline void setWhereExpression(std::unique_ptr<ParsedExpression> expression) {
        whereExpression = std::move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }

    inline ParsedExpression* getWhereExpression() const { return whereExpression.get(); }

private:
    std::unique_ptr<ParsedExpression> whereExpression;
};

} // namespace parser
} // namespace kuzu
