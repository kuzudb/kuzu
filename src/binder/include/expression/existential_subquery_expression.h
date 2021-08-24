#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/logical_plan.h"

using namespace graphflow::planner;

namespace graphflow {
namespace binder {

class ExistentialSubqueryExpression : public Expression {

public:
    ExistentialSubqueryExpression(unique_ptr<BoundSingleQuery> subquery)
        : Expression{EXISTENTIAL_SUBQUERY, BOOL}, subquery{move(subquery)} {

    }

    // NOTE: remove once subquery enumeration is added
    ExistentialSubqueryExpression(unique_ptr<LogicalPlan> subPlan)
        : Expression{EXISTENTIAL_SUBQUERY, BOOL}, subPlan{move(subPlan)} {}

    inline BoundSingleQuery* getSubquery() { return subquery.get(); }
    inline void setSubPlan(unique_ptr<LogicalPlan> plan) { subPlan = move(plan); }
    inline unique_ptr<LogicalPlan> moveSubPlan() { return move(subPlan); }

    vector<const Expression*> getIncludedExpressionsWithTypes(
        const unordered_set<ExpressionType>& expressionTypes) const override;

private:
    unique_ptr<BoundSingleQuery> subquery;
    unique_ptr<LogicalPlan> subPlan;
};

} // namespace binder
} // namespace graphflow
