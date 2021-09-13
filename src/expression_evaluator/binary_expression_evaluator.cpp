#include "src/expression_evaluator/include/binary_expression_evaluator.h"

namespace graphflow {
namespace evaluator {

BinaryExpressionEvaluator::BinaryExpressionEvaluator(unique_ptr<ExpressionEvaluator> leftExpr,
    unique_ptr<ExpressionEvaluator> rightExpr, ExpressionType expressionType, DataType dataType)
    : ExpressionEvaluator{expressionType, dataType} {
    childrenExpr.push_back(move(leftExpr));
    childrenExpr.push_back(move(rightExpr));
    executeOperation = getBinaryVectorExecuteOperation(expressionType);
    if (dataType == BOOL) {
        selectOperation = getBinaryVectorSelectOperation(expressionType);
    }
}

void BinaryExpressionEvaluator::initResultSet(
    const ResultSet& resultSet, MemoryManager& memoryManager) {
    childrenExpr[0]->initResultSet(resultSet, memoryManager);
    childrenExpr[1]->initResultSet(resultSet, memoryManager);
    result = createResultValueVector(memoryManager);
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
        resultVector = make_shared<ValueVector>(&memoryManager, dataType);
        resultVector->state = DataChunkState::getSingleValueDataChunkState();
    } else {
        resultVector = make_shared<ValueVector>(&memoryManager, dataType);
        auto& unFlatVector = !isLeftResultFlat ? childrenExpr[0]->result : childrenExpr[1]->result;
        resultVector->state = unFlatVector->state;
    }
    return resultVector;
}

unique_ptr<ExpressionEvaluator> BinaryExpressionEvaluator::clone() {
    return make_unique<BinaryExpressionEvaluator>(
        childrenExpr[0]->clone(), childrenExpr[1]->clone(), expressionType, dataType);
}

} // namespace evaluator
} // namespace graphflow
