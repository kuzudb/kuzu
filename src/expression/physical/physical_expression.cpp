#include "src/expression/include/physical/physical_expression.h"

namespace graphflow {
namespace expression {

bool PhysicalExpression::isResultFlat() {
    if (childrenExpr.empty()) {
        return result->state->isFlat();
    }
    for (auto& expr : childrenExpr) {
        if (!expr->isResultFlat()) {
            return false;
        }
    }
    return true;
}

} // namespace expression
} // namespace graphflow
