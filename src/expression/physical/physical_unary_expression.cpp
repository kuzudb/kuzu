#include "src/expression/include/physical/physical_unary_expression.h"

namespace graphflow {
namespace expression {

PhysicalUnaryExpression::PhysicalUnaryExpression(
    unique_ptr<PhysicalExpression> child, const EXPRESSION_TYPE& expressionType) {
    operands.push_back(child->getResult());
    childrenExpr.push_back(move(child));
    operation = ValueVector::getUnaryOperation(expressionType);
    result = createResultValueVector(
        getUnaryExpressionResultDataType(expressionType, childrenExpr[0]->getResultDataType()));
}

void PhysicalUnaryExpression::evaluate() {
    childrenExpr[0]->evaluate();
    operation(*operands[0], *result);
}

} // namespace expression
} // namespace graphflow
