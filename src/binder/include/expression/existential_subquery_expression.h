#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

class BoundSingleQuery;

class ExistentialSubqueryExpression : public Expression {

public:
    ExistentialSubqueryExpression(unique_ptr<BoundSingleQuery> subquery)
        : Expression{EXISTENTIAL_SUBQUERY, BOOL}, subquery{move(subquery)} {}

    inline BoundSingleQuery* getSubquery() { return subquery.get(); };

private:
    unique_ptr<BoundSingleQuery> subquery;
};

} // namespace binder
} // namespace graphflow
