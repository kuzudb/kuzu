#include "planner/logical_plan/logical_operator/logical_unwind.h"

namespace kuzu {
namespace planner {

void LogicalUnwind::computeSchema() {
    copyChildSchema(0);
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(aliasExpression, groupPos);
}

} // namespace planner
} // namespace kuzu
