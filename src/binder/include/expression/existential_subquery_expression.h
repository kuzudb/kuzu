#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/planner/include/norm_query/normalized_query.h"

using namespace graphflow::planner;

namespace graphflow {
namespace binder {

class ExistentialSubqueryExpression : public Expression {

public:
    ExistentialSubqueryExpression(unique_ptr<BoundSingleQuery> boundSubquery)
        : Expression{EXISTENTIAL_SUBQUERY, BOOL}, boundSubquery{move(boundSubquery)} {}

    inline BoundSingleQuery* getBoundSubquery() { return boundSubquery.get(); }

    inline void setNormalizedSubquery(unique_ptr<NormalizedQuery> normalizedQuery) {
        normalizedSubquery = move(normalizedQuery);
    }
    inline NormalizedQuery* getNormalizedSubquery() { return normalizedSubquery.get(); }

    inline void setSubPlan(unique_ptr<LogicalPlan> plan) { subPlan = move(plan); }
    inline bool hasSubPlan() { return subPlan != nullptr; }
    inline unique_ptr<LogicalPlan> getSubPlan() { return subPlan->copy(); }

    vector<shared_ptr<Expression>> getDependentVariables() override;
    vector<shared_ptr<Expression>> getDependentProperties() override;
    vector<shared_ptr<Expression>> getDependentLeafExpressions() override;

private:
    unique_ptr<BoundSingleQuery> boundSubquery;
    unique_ptr<NormalizedQuery> normalizedSubquery;
    unique_ptr<LogicalPlan> subPlan;
};

} // namespace binder
} // namespace graphflow
