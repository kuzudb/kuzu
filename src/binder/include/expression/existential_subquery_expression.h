#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/logical_plan.h"

using namespace graphflow::planner;

namespace graphflow {
namespace binder {

class ExistentialSubqueryExpression : public Expression {

public:
    ExistentialSubqueryExpression(shared_ptr<BoundSingleQuery> subquery)
        : Expression{EXISTENTIAL_SUBQUERY, BOOL}, subquery{move(subquery)} {}

    inline BoundSingleQuery* getSubquery() { return subquery.get(); }
    inline void setSubPlan(unique_ptr<LogicalPlan> plan) { subPlan = move(plan); }
    inline unique_ptr<LogicalPlan> moveSubPlan() { return move(subPlan); }

    vector<shared_ptr<Expression>> getIncludedExpressionsWithTypes(
        const unordered_set<ExpressionType>& expressionTypes) override;

    unique_ptr<Expression> copy() override {
        auto expression = make_unique<ExistentialSubqueryExpression>(subquery);
        if (subPlan) {
            expression->setSubPlan(subPlan->copy());
        }
        return expression;
    }

private:
    shared_ptr<BoundSingleQuery> subquery;
    unique_ptr<LogicalPlan> subPlan;
};

} // namespace binder
} // namespace graphflow
