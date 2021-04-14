#include "src/expression/include/physical/physical_unary_expression.h"

namespace graphflow {
namespace expression {

PhysicalUnaryExpression::PhysicalUnaryExpression(
    unique_ptr<PhysicalExpression> child, ExpressionType expressionType) {
    childrenExpr.push_back(move(child));
    this->expressionType = expressionType;
    operation = ValueVector::getUnaryOperation(expressionType);
    result = createResultValueVector(
        getUnaryExpressionResultDataType(expressionType, childrenExpr[0]->dataType));
}

void PhysicalUnaryExpression::evaluate() {
    childrenExpr[0]->evaluate();
    operation(*childrenExpr[0]->result, *result);
}

void PhysicalUnaryExpression::setExpressionInputDataChunk(shared_ptr<DataChunk> dataChunk) {
    childrenExpr[0]->setExpressionInputDataChunk(dataChunk);
}
void PhysicalUnaryExpression::setExpressionResultOwnerState(
    shared_ptr<DataChunkState> dataChunkState) {
    result->setDataChunkOwnerState(dataChunkState);
    childrenExpr[0]->setExpressionResultOwnerState(dataChunkState);
}

} // namespace expression
} // namespace graphflow
