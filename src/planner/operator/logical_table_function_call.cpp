#include "planner/operator/logical_table_function_call.h"

namespace kuzu {
namespace planner {

void LogicalTableFunctionCall::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto i = 0u; i < columns.size(); ++i) {
        schema->insertToGroupAndScope(columns[i], 0);
        if (*columns[i] != *castedColumns[i]) {
            schema->insertToGroupAndScope(castedColumns[i], 0);
        }
    }
    if (!castedColumns.empty()) {
        KU_ASSERT(columns.size() == castedColumns.size());
        for (auto i = 0u; i < columns.size(); ++i) {
            if (*columns[i] != *castedColumns[i]) {
                schema->insertToGroupAndScope(castedColumns[i], 0);
            }
        }
    }
    if (offset != nullptr) {
        schema->insertToGroupAndScope(offset, 0);
    }
}

void LogicalTableFunctionCall::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto i = 0u; i < columns.size(); ++i) {
        schema->insertToGroupAndScope(columns[i], groupPos);
    }
    if (!castedColumns.empty()) {
        KU_ASSERT(columns.size() == castedColumns.size());
        for (auto i = 0u; i < columns.size(); ++i) {
            if (*columns[i] != *castedColumns[i]) {
                schema->insertToGroupAndScope(castedColumns[i], groupPos);
            }
        }
    }
    if (offset != nullptr) {
        schema->insertToGroupAndScope(offset, groupPos);
    }
}

} // namespace planner
} // namespace kuzu
