#include "planner/operator/logical_table_function_call.h"

namespace kuzu {
namespace planner {

void LogicalTableFunctionCall::computeFlatSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& expr : columns) {
        schema->insertToGroupAndScope(expr, groupPos);
    }
    if (offset != nullptr) {
        schema->insertToGroupAndScope(offset, groupPos);
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
