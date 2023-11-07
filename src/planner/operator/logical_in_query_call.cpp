#include "planner/operator/logical_in_query_call.h"

namespace kuzu {
namespace planner {

void LogicalInQueryCall::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& outputExpression : outputExpressions) {
        schema->insertToGroupAndScope(outputExpression, 0);
    }
    schema->insertToGroupAndScope(rowIDExpression, 0);
}

void LogicalInQueryCall::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& outputExpression : outputExpressions) {
        schema->insertToGroupAndScope(outputExpression, groupPos);
    }
    schema->insertToGroupAndScope(rowIDExpression, groupPos);
}

} // namespace planner
} // namespace kuzu
