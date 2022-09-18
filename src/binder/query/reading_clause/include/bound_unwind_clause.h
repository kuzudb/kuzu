#pragma once

#include "bound_reading_clause.h"

#include "src/binder/expression/include/expression.h"

namespace graphflow {
namespace binder {

class BoundUnwindClause : public BoundReadingClause {

public:
    explicit BoundUnwindClause(shared_ptr<Expression> literalExpression, string listAlias)
        : BoundReadingClause{ClauseType::UNWIND},
          expression{move(literalExpression)}, alias{move(listAlias)} {}

    ~BoundUnwindClause() = default;

    inline shared_ptr<Expression> getExpression() const { return expression; }

    inline string getAlias() const { return alias; }

    inline unique_ptr<BoundReadingClause> copy() override {
        return make_unique<BoundUnwindClause>(*this);
    }

private:
    shared_ptr<Expression> expression;
    string alias;
};
} // namespace binder
} // namespace graphflow
