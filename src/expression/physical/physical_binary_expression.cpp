#include "src/expression/include/physical/physical_binary_expression.h"

namespace graphflow {
namespace expression {

PhysicalBinaryExpression::PhysicalBinaryExpression(unique_ptr<PhysicalExpression> leftExpr,
    unique_ptr<PhysicalExpression> rightExpr, ExpressionType expressionType) {
    operands.push_back(leftExpr->result);
    operands.push_back(rightExpr->result);
    childrenExpr.push_back(move(leftExpr));
    childrenExpr.push_back(move(rightExpr));
    this->expressionType = expressionType;
    operation = ValueVector::getBinaryOperation(expressionType);
    result = createResultValueVector(getBinaryExpressionResultDataType(expressionType,
        childrenExpr[0]->result->getDataType(), childrenExpr[1]->result->getDataType()));
}

void PhysicalBinaryExpression::evaluate() {
    childrenExpr[0]->evaluate();
    childrenExpr[1]->evaluate();
    operation(*operands[0], *operands[1], *result);
}

} // namespace expression
} // namespace graphflow
