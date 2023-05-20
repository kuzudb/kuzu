#pragma once

#include "binder/expression/expression.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundUnwindClause : public BoundReadingClause {
public:
    BoundUnwindClause(
        std::shared_ptr<Expression> expression, std::shared_ptr<Expression> aliasExpression)
        : BoundReadingClause{common::ClauseType::UNWIND}, expression{std::move(expression)},
          aliasExpression{std::move(aliasExpression)} {}

    ~BoundUnwindClause() override = default;

    inline std::shared_ptr<Expression> getExpression() const { return expression; }

    inline std::shared_ptr<Expression> getAliasExpression() const { return aliasExpression; }

    inline std::unique_ptr<BoundReadingClause> copy() override {
        return std::make_unique<BoundUnwindClause>(*this);
    }

private:
    std::shared_ptr<Expression> expression;
    std::shared_ptr<Expression> aliasExpression;
};
} // namespace binder
} // namespace kuzu
