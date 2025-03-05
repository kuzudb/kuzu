#include "planner/operator/extend/logical_recursive_extend.h"

namespace kuzu {
namespace planner {

void LogicalRecursiveExtend::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& expr : info.outExprs) {
        schema->insertToGroupAndScope(expr, 0);
    }
}

void LogicalRecursiveExtend::computeFactorizedSchema() {
    createEmptySchema();
    auto pos = schema->createGroup();
    for (auto& e : info.outExprs) {
        schema->insertToGroupAndScope(e, pos);
    }
}

} // namespace planner
} // namespace kuzu
