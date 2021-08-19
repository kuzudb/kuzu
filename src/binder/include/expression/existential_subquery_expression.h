#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/logical_plan.h"

namespace graphflow {
namespace binder {

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
