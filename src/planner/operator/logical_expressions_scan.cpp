#include "planner/logical_plan/logical_operator/logical_expressions_scan.h"

namespace kuzu {
namespace planner {

void LogicalExpressionsScan::computeSchema() {
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

} // namespace planner
} // namespace kuzu
