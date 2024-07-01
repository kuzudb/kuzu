#include "planner/operator/logical_gds_call.h"

#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {

void LogicalGDSCall::computeFlatSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    auto srcInternalIDExpr =
        info.srcNodeIDExpression->constPtrCast<binder::NodeExpression>()->getInternalID();
    schema->insertToGroupAndScope(srcInternalIDExpr, groupPos);
    for (auto& expr : info.outExpressions) {
        if (expr->getUniqueName() != srcInternalIDExpr->getUniqueName()) {
            schema->insertToGroupAndScope(expr, groupPos);
        }
    }
}

void LogicalGDSCall::computeFactorizedSchema() {
    createEmptySchema();
    auto srcNodeGroupPos = schema->createGroup();
    auto srcInternalIDExpr =
        info.srcNodeIDExpression->constPtrCast<binder::NodeExpression>()->getInternalID();
    schema->insertToGroupAndScope(srcInternalIDExpr, srcNodeGroupPos);
    schema->flattenGroup(srcNodeGroupPos);
    auto pos = schema->createGroup();
    for (auto& e : info.outExpressions) {
        if (e->getUniqueName() != srcInternalIDExpr->getUniqueName())
            schema->insertToGroupAndScope(e, pos);
    }
}

} // namespace planner
} // namespace kuzu
