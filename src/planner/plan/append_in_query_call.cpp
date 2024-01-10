#include "binder/query/reading_clause/bound_in_query_call.h"
#include "planner/operator/logical_in_query_call.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendInQueryCall(const BoundReadingClause& boundReadingClause, LogicalPlan& plan) {
    auto& boundInQueryCall = (BoundInQueryCall&)boundReadingClause;
    auto logicalInQueryCall = make_shared<LogicalInQueryCall>(boundInQueryCall.getTableFunc(),
        boundInQueryCall.getBindData()->copy(), boundInQueryCall.getOutExprs(),
        boundInQueryCall.getRowIdxExpr());
    logicalInQueryCall->computeFactorizedSchema();
    plan.setLastOperator(logicalInQueryCall);
}

} // namespace planner
} // namespace kuzu
