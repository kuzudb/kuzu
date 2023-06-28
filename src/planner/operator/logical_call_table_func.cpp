#include "planner/logical_plan/logical_operator/logical_call_table_func.h"

namespace kuzu {
namespace planner {

void LogicalCallTableFunc::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& outputExpression : outputExpressions) {
        schema->insertToGroupAndScope(outputExpression, 0);
    }
}

void LogicalCallTableFunc::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& outputExpression : outputExpressions) {
        schema->insertToGroupAndScope(outputExpression, groupPos);
    }
}

} // namespace planner
} // namespace kuzu
