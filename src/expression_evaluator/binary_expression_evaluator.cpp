#include "src/expression_evaluator/include/binary_expression_evaluator.h"

namespace graphflow {
namespace evaluator {

BinaryExpressionEvaluator::BinaryExpressionEvaluator(MemoryManager& memoryManager,
    unique_ptr<ExpressionEvaluator> leftExpr, unique_ptr<ExpressionEvaluator> rightExpr,
    ExpressionType expressionType, DataType dataType) {
    childrenExpr.push_back(move(leftExpr));
    childrenExpr.push_back(move(rightExpr));
    this->expressionType = expressionType;
    this->dataType = dataType;
    operation = getBinaryOperation(expressionType);
    result = createResultValueVector(memoryManager);
}

void BinaryExpressionEvaluator::evaluate() {
    childrenExpr[0]->evaluate();
    childrenExpr[1]->evaluate();
    operation(*childrenExpr[0]->result, *childrenExpr[1]->result, *result);
}

shared_ptr<ValueVector> BinaryExpressionEvaluator::createResultValueVector(
    MemoryManager& memoryManager) {
    auto valueVector = make_shared<ValueVector>(&memoryManager, dataType);
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

} // namespace evaluator
} // namespace graphflow
