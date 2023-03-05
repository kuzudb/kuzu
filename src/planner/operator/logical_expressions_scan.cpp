#include "planner/logical_plan/logical_operator/logical_expressions_scan.h"

namespace kuzu {
namespace planner {

void LogicalExpressionsScan::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->setGroupAsSingleState(groupPos); // Mark group holding constant as single state.
    for (auto& expression : expressions) {
        // No need to insert repeated constant.
        if (schema->isExpressionInScope(*expression)) {
            continue;
        }
        schema->insertToGroupAndScope(expression, groupPos);
    }
}

void LogicalExpressionsScan::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& expression : expressions) {
        // No need to insert repeated constant.
        if (schema->isExpressionInScope(*expression)) {
            continue;
        }
        schema->insertToGroupAndScope(expression, 0);
    }
}

} // namespace planner
} // namespace kuzu
