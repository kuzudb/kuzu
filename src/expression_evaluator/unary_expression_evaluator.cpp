#include "src/expression_evaluator/include/unary_expression_evaluator.h"

namespace graphflow {
namespace evaluator {

UnaryExpressionEvaluator::UnaryExpressionEvaluator(MemoryManager& memoryManager,
    unique_ptr<ExpressionEvaluator> child, ExpressionType expressionType, DataType dataType,
    bool isSelectOperation)
    : ExpressionEvaluator{expressionType, dataType} {
    childrenExpr.push_back(move(child));
    this->expressionType = expressionType;
    this->dataType = dataType;
    if (isSelectOperation) {
        selectOperation = getUnaryVectorSelectOperation(expressionType);
    } else {
        executeOperation = getUnaryVectorExecuteOperation(expressionType);
        result = createResultValueVector(memoryManager);
    }
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
        resultVector = make_shared<ValueVector>(&memoryManager, dataType, true /* isSingleValue */);
        resultVectorCapacity = 1;
        resultVector->state = DataChunkState::getSingleValueDataChunkState();
    } else {
        resultVector =
            make_shared<ValueVector>(&memoryManager, dataType, false /* isSingleValue */);
        resultVector->state = childrenExpr[0]->result->state;
        resultVectorCapacity = DEFAULT_VECTOR_CAPACITY;
    }
    if (expressionType == CAST_TO_UNSTRUCTURED_VECTOR) {
        auto unstructuredValues = (Value*)resultVector->values;
        for (auto i = 0u; i < resultVectorCapacity; i++) {
            unstructuredValues[i].dataType = childrenExpr[0]->dataType;
        }
    }
    resultVector->setNullMask(childrenExpr[0]->result->getNullMask());
    return resultVector;
}

} // namespace evaluator
} // namespace graphflow
