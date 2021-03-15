#include "src/expression/include/physical/physical_expression.h"

namespace graphflow {
namespace expression {

bool PhysicalExpression::isResultFlat() {
    for (auto& expr : childrenExpr) {
        if (expr->isResultFlat()) {
            return true;
        }
    }
    return false;
}

shared_ptr<ValueVector> PhysicalExpression::createResultValueVector(DataType dataType) {
    return isResultFlat() ? make_shared<ValueVector>(dataType, 1) :
                            make_shared<ValueVector>(dataType);
}

} // namespace expression
} // namespace graphflow
