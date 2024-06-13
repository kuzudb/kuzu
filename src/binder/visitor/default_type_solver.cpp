#include "binder/visitor/default_type_solver.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

static void resolveAnyType(Expression& expr) {
    if (expr.getDataType().getLogicalTypeID() != LogicalTypeID::ANY) {
        return;
    }
    expr.cast(LogicalType::STRING());
}

void DefaultTypeSolver::visitProjectionBody(const BoundProjectionBody& projectionBody) {
    for (auto& expr : projectionBody.getProjectionExpressions()) {
        resolveAnyType(*expr);
    }
    for (auto& expr : projectionBody.getOrderByExpressions()) {
        resolveAnyType(*expr);
    }
}

} // namespace binder
} // namespace kuzu
