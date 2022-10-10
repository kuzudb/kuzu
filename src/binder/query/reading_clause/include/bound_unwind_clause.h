#pragma once

#include "bound_reading_clause.h"

#include "src/binder/expression/include/expression.h"

namespace graphflow {
namespace binder {

class BoundUnwindClause : public BoundReadingClause {

public:
    explicit BoundUnwindClause(shared_ptr<Expression> literalExpression,
        shared_ptr<Expression> aliasExpression, string listAlias)
        : BoundReadingClause{ClauseType::UNWIND}, expression{move(literalExpression)},
          aliasExpression{move(aliasExpression)}, alias{move(listAlias)} {}

    ~BoundUnwindClause() = default;

    inline shared_ptr<Expression> getExpression() const { return expression; }

    inline shared_ptr<Expression> getAliasExpression() const { return aliasExpression; }

    inline string getAlias() const { return alias; }

    inline unique_ptr<BoundReadingClause> copy() override {
        return make_unique<BoundUnwindClause>(*this);
    }

private:
    shared_ptr<Expression> expression;
    shared_ptr<Expression> aliasExpression;
    string alias;
};
} // namespace binder
} // namespace graphflow
