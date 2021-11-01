#include "src/expression_evaluator/include/unary_expression_evaluator.h"

namespace graphflow {
namespace evaluator {

UnaryExpressionEvaluator::UnaryExpressionEvaluator(
    unique_ptr<ExpressionEvaluator> child, ExpressionType expressionType, DataType dataType)
    : ExpressionEvaluator{expressionType, dataType} {
    childrenExpr.push_back(move(child));
    executeOperation = getUnaryVectorExecuteOperation(expressionType);
    if (dataType == BOOL) {
        selectOperation = getUnaryVectorSelectOperation(expressionType);
    }
}

void UnaryExpressionEvaluator::initResultSet(
    const ResultSet& resultSet, MemoryManager& memoryManager) {
    childrenExpr[0]->initResultSet(resultSet, memoryManager);
    result = createResultValueVector(memoryManager);
}

void UnaryExpressionEvaluator::evaluate() {
    childrenExpr[0]->evaluate();
    executeOperation(*childrenExpr[0]->result, *result);
}

uint64_t UnaryExpressionEvaluator::select(sel_t* selectedPositions) {
    childrenExpr[0]->evaluate();
    return selectOperation(*childrenExpr[0]->result, selectedPositions);
}

shared_ptr<ValueVector> UnaryExpressionEvaluator::createResultValueVector(
    MemoryManager& memoryManager) {
    shared_ptr<ValueVector> resultVector;
    uint64_t resultVectorCapacity;
    if (childrenExpr[0]->isResultFlat()) {
        resultVector = make_shared<ValueVector>(&memoryManager, dataType, true /* isSequence */);
        resultVectorCapacity = 1;
        resultVector->state = DataChunkState::getSingleValueDataChunkState();
    } else {
        resultVector = make_shared<ValueVector>(&memoryManager, dataType, false /* isSequence */);
        resultVector->state = childrenExpr[0]->result->state;
        resultVectorCapacity = DEFAULT_VECTOR_CAPACITY;
    }
    if (expressionType == CAST_TO_UNSTRUCTURED_VALUE) {
        auto unstructuredValues = (Value*)resultVector->values;
        for (auto i = 0u; i < resultVectorCapacity; i++) {
            unstructuredValues[i].dataType = childrenExpr[0]->dataType;
        }
    }
    resultVector->setNullMask(childrenExpr[0]->result->getNullMask());
    return resultVector;
}

unique_ptr<ExpressionEvaluator> UnaryExpressionEvaluator::clone() {
    return make_unique<UnaryExpressionEvaluator>(
        childrenExpr[0]->clone(), expressionType, dataType);
}

} // namespace evaluator
} // namespace graphflow
