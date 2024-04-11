#include "binder/query/reading_clause/bound_in_query_call.h"
#include "planner/operator/logical_in_query_call.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::shared_ptr<LogicalOperator> Planner::getCall(const BoundReadingClause& readingClause) {
    auto& call = readingClause.constCast<BoundInQueryCall>();
    return std::make_shared<LogicalInQueryCall>(call.getTableFunc(), call.getBindData()->copy(),
        call.getOutExprs(), call.getRowIdxExpr());
}

} // namespace planner
} // namespace kuzu
