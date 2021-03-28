#include "src/expression/include/physical/physical_unary_expression.h"

namespace graphflow {
namespace expression {

PhysicalUnaryExpression::PhysicalUnaryExpression(
    unique_ptr<PhysicalExpression> child, ExpressionType expressionType) {
    operands.push_back(child->result);
    childrenExpr.push_back(move(child));
    this->expressionType = expressionType;
    operation = ValueVector::getUnaryOperation(expressionType);
    result = createResultValueVector(
        getUnaryExpressionResultDataType(expressionType, childrenExpr[0]->result->getDataType()));
}

void PhysicalUnaryExpression::evaluate() {
    childrenExpr[0]->evaluate();
    operation(*operands[0], *result);
}

} // namespace expression
} // namespace graphflow
