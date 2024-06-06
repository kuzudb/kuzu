#include "planner/operator/logical_gds_call.h"

namespace kuzu {
namespace planner {

void LogicalGDSCall::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& expr : info.outExprs) {
        schema->insertToGroupAndScope(expr, 0);
    }
}

void LogicalGDSCall::computeFactorizedSchema() {
    createEmptySchema();
    auto pos = schema->createGroup();
    for (auto& e : info.outExprs) {
        schema->insertToGroupAndScope(e, pos);
    }
}

} // namespace planner
} // namespace kuzu
