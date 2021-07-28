#include "src/expression_evaluator/include/binary_expression_evaluator.h"

namespace graphflow {
namespace evaluator {

BinaryExpressionEvaluator::BinaryExpressionEvaluator(MemoryManager& memoryManager,
    unique_ptr<ExpressionEvaluator> leftExpr, unique_ptr<ExpressionEvaluator> rightExpr,
    ExpressionType expressionType, DataType dataType, bool isSelectOperation)
    : ExpressionEvaluator{expressionType, dataType} {
    childrenExpr.push_back(move(leftExpr));
    childrenExpr.push_back(move(rightExpr));
    if (isSelectOperation) {
        selectOperation = getBinaryVectorSelectOperation(expressionType);
    } else {
        executeOperation = getBinaryVectorExecuteOperation(expressionType);
        result = createResultValueVector(memoryManager);
    }
}

void BinaryExpressionEvaluator::evaluate() {
    childrenExpr[0]->evaluate();
    childrenExpr[1]->evaluate();
    executeOperation(*childrenExpr[0]->result, *childrenExpr[1]->result, *result);
}

uint64_t BinaryExpressionEvaluator::select(sel_t* selectedPositions) {
    childrenExpr[0]->evaluate();
    childrenExpr[1]->evaluate();
    return selectOperation(*childrenExpr[0]->result, *childrenExpr[1]->result, selectedPositions);
}

shared_ptr<ValueVector> BinaryExpressionEvaluator::createResultValueVector(
    MemoryManager& memoryManager) {
    shared_ptr<ValueVector> resultVector;
    auto isLeftResultFlat = childrenExpr[0]->isResultFlat();
    auto isRightResultFlat = childrenExpr[1]->isResultFlat();
    // The binary expression result is flat only when both left and right are flat.
    if (isLeftResultFlat && isRightResultFlat) {
        resultVector = make_shared<ValueVector>(&memoryManager, dataType, true /* isSingleValue */);
        resultVector->state = DataChunkState::getSingleValueDataChunkState();
    } else {
        resultVector =
            make_shared<ValueVector>(&memoryManager, dataType, false /* isSingleValue */);
        auto& unFlatVector = !isLeftResultFlat ? childrenExpr[0]->result : childrenExpr[1]->result;
        resultVector->state = unFlatVector->state;
    }
    return resultVector;
}

} // namespace evaluator
} // namespace graphflow
