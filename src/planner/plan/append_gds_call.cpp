#include "binder/query/reading_clause/bound_gds_call.h"
#include "planner/operator/logical_gds_call.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::shared_ptr<LogicalOperator> Planner::getGDSCall(const BoundGDSCallInfo& info) {
    return std::make_shared<LogicalGDSCall>(info.copy());
}

} // namespace planner
} // namespace kuzu
