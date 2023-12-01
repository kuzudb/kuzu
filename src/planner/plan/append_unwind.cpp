#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "planner/operator/logical_unwind.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void QueryPlanner::appendUnwind(const BoundReadingClause& boundReadingClause, LogicalPlan& plan) {
    auto& boundUnwindClause = (BoundUnwindClause&)boundReadingClause;
    auto unwind = make_shared<LogicalUnwind>(boundUnwindClause.getExpression(),
        boundUnwindClause.getAliasExpression(), plan.getLastOperator());
    QueryPlanner::appendFlattens(unwind->getGroupsPosToFlatten(), plan);
    unwind->setChild(0, plan.getLastOperator());
    unwind->computeFactorizedSchema();
    plan.setLastOperator(unwind);
}

} // namespace planner
} // namespace kuzu
