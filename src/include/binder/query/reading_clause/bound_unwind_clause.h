#pragma once

#include "binder/expression/expression.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundUnwindClause : public BoundReadingClause {

public:
    explicit BoundUnwindClause(
        shared_ptr<Expression> expression, shared_ptr<Expression> aliasExpression)
        : BoundReadingClause{ClauseType::UNWIND}, expression{std::move(expression)},
          aliasExpression{std::move(aliasExpression)} {}

    ~BoundUnwindClause() = default;

    inline shared_ptr<Expression> getExpression() const { return expression; }

    inline bool hasExpression() const { return expression != nullptr; }

    inline shared_ptr<Expression> getAliasExpression() const { return aliasExpression; }

    inline expression_vector getSubPropertyExpressions() const override {
        expression_vector expressions;
        if (hasExpression()) {
            for (auto& property : getExpression()->getSubPropertyExpressions()) {
                expressions.push_back(property);
            }
        }
        return expressions;
    }

    inline unique_ptr<BoundReadingClause> copy() override {
        return make_unique<BoundUnwindClause>(*this);
    }

private:
    shared_ptr<Expression> expression;
    shared_ptr<Expression> aliasExpression;
};
} // namespace binder
} // namespace kuzu
