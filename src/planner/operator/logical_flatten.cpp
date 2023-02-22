#include "planner/logical_plan/logical_operator/logical_flatten.h"

namespace kuzu {
namespace planner {

void LogicalFlatten::computeSchema() {
    copyChildSchema(0);
    schema->flattenGroup(groupPos);
}

} // namespace planner
} // namespace kuzu
