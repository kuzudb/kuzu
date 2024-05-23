#include "planner/operator/logical_table_function_call.h"

namespace kuzu {
namespace planner {

void LogicalTableFunctionCall::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& expr : outputExpressions) {
        schema->insertToGroupAndScope(expr, 0);
    }
    schema->insertToGroupAndScope(rowIDExpression, 0);
}

void LogicalTableFunctionCall::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& expr : outputExpressions) {
        schema->insertToGroupAndScope(expr, groupPos);
    }
    schema->insertToGroupAndScope(rowIDExpression, groupPos);
}

} // namespace planner
} // namespace kuzu
