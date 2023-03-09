#include "planner/logical_plan/logical_operator/logical_flatten.h"

namespace kuzu {
namespace planner {

void LogicalFlatten::computeFactorizedSchema() {
    copyChildSchema(0);
    schema->flattenGroup(groupPos);
}

void LogicalFlatten::computeFlatSchema() {
    throw common::InternalException("LogicalFlatten::computeFlatSchema() should never be used.");
}

} // namespace planner
} // namespace kuzu
