#include "src/expression/include/physical/physical_binary_expression.h"

namespace graphflow {
namespace expression {

PhysicalBinaryExpression::PhysicalBinaryExpression(unique_ptr<PhysicalExpression> leftExpr,
    unique_ptr<PhysicalExpression> rightExpr, const EXPRESSION_TYPE& expressionType) {
    operands.push_back(leftExpr->getResult());
    operands.push_back(rightExpr->getResult());
    childrenExpr.push_back(move(leftExpr));
    childrenExpr.push_back(move(rightExpr));
    operation = ValueVector::getBinaryOperation(expressionType);
    result = createResultValueVector(getBinaryExpressionResultDataType(expressionType,
        childrenExpr[0]->getResultDataType(), childrenExpr[1]->getResultDataType()));
}

void PhysicalBinaryExpression::evaluate() {
    childrenExpr[0]->evaluate();
    childrenExpr[1]->evaluate();
    operation(*operands[0], *operands[1], *result);
}

} // namespace expression
} // namespace graphflow
