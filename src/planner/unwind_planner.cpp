#include "include/unwind_planner.h"

#include "include/enumerator.h"
#include "logical_plan/logical_operator/include/logical_unwind.h"

namespace graphflow {
namespace planner {

void UnwindPlanner::planUnwindClause(BoundUnwindClause& boundUnwindClause, LogicalPlan& plan) {
    if (plan.isEmpty()) { // UNWIND [1, 2, 3, 4] AS x RETURN x => append expression scan operator
                          // for [1, 2, 3, 4]
        expression_vector expressions;
        expressions.push_back(boundUnwindClause.getExpression());
        Enumerator::appendExpressionsScan(expressions, plan);
        appendUnwindClause(boundUnwindClause, plan);
    } else {
        appendUnwindClause(boundUnwindClause, plan);
    }
}

void UnwindPlanner::appendUnwindClause(BoundUnwindClause& boundUnwindClause, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(boundUnwindClause.getAliasExpression(), groupPos);
    auto logicalUnwind = make_shared<LogicalUnwind>(boundUnwindClause.getExpression(),
        boundUnwindClause.getAliasExpression(), plan.getLastOperator());
    plan.appendOperator(logicalUnwind);
}

} // namespace planner
} // namespace graphflow
