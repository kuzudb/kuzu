#include "planner/operator/logical_table_function_call.h"

namespace kuzu {
namespace planner {

void LogicalTableFunctionCall::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& expr : columns) {
        schema->insertToGroupAndScope(expr, 0);
    }
    if (offset != nullptr) {
        schema->insertToGroupAndScope(offset, 0);
    }
}

void LogicalTableFunctionCall::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& expr : columns) {
        schema->insertToGroupAndScope(expr, groupPos);
    }
    if (offset != nullptr) {
        schema->insertToGroupAndScope(offset, groupPos);
    }
}

} // namespace planner
} // namespace kuzu
