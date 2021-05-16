#include "src/expression/include/physical/physical_binary_expression.h"

namespace graphflow {
namespace expression {

PhysicalBinaryExpression::PhysicalBinaryExpression(unique_ptr<PhysicalExpression> leftExpr,
    unique_ptr<PhysicalExpression> rightExpr, ExpressionType expressionType, DataType dataType) {
    childrenExpr.push_back(move(leftExpr));
    childrenExpr.push_back(move(rightExpr));
    this->expressionType = expressionType;
    this->dataType = dataType;
    operation = getBinaryOperation(expressionType);
    result = createResultValueVector();
}

void PhysicalBinaryExpression::evaluate() {
    childrenExpr[0]->evaluate();
    childrenExpr[1]->evaluate();
    operation(*childrenExpr[0]->result, *childrenExpr[1]->result, *result);
}

shared_ptr<ValueVector> PhysicalBinaryExpression::createResultValueVector() {
    auto valueVector = make_shared<ValueVector>(dataType, MAX_VECTOR_SIZE);
    auto isLeftResultFlat = childrenExpr[0]->isResultFlat();
    auto isRightResultFlat = childrenExpr[1]->isResultFlat();
    if (isLeftResultFlat && isRightResultFlat) {
        valueVector->state = VectorState::getSingleValueDataChunkState();
    } else {
        auto& unflatVector = !isLeftResultFlat ? childrenExpr[0]->result : childrenExpr[1]->result;
        valueVector->state = unflatVector->state;
    }
    return valueVector;
}

} // namespace expression
} // namespace graphflow
