#include "binder/query/reading_clause/bound_gds_call.h"
#include "planner/operator/logical_gds_call.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::shared_ptr<LogicalOperator> Planner::getGDSCall(const BoundReadingClause& readingClause) {
    auto& call = readingClause.constCast<BoundGDSCall>();
    return std::make_shared<LogicalGDSCall>(call.getInfo().copy());
}

} // namespace planner
} // namespace kuzu
