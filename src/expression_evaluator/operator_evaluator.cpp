#include "include/operator_evaluator.h"

#include "src/common/include/vector/operations/vector_arithmetic_operations.h"
#include "src/function/cast/include/vector_cast_operations.h"

using namespace graphflow::function;

namespace graphflow {
namespace evaluator {

void OperatorExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    BaseExpressionEvaluator::init(resultSet, memoryManager);
    getExecOperation();
}

void UnaryOperatorExpressionEvaluator::init(
    const ResultSet& resultSet, MemoryManager* memoryManager) {
    OperatorExpressionEvaluator::init(resultSet, memoryManager);
    resultVector = make_shared<ValueVector>(memoryManager, expression->dataType);
    resultVector->state = children[0]->resultVector->state;
}

void UnaryOperatorExpressionEvaluator::getExecOperation() {
    switch (expression->expressionType) {
    case NEGATE: {
        execOperation = VectorArithmeticOperations::Negate;
    } break;
    case CAST_TO_UNSTRUCTURED_VALUE: {
        execOperation = VectorCastOperations::castStructuredToUnstructuredValue;
    } break;
    case ABS_FUNC: {
        execOperation = VectorArithmeticOperations::Abs;
    } break;
    case FLOOR_FUNC: {
        execOperation = VectorArithmeticOperations::Floor;
    } break;
    case CEIL_FUNC: {
        execOperation = VectorArithmeticOperations::Ceil;
    } break;
    default:
        throw invalid_argument(
            "Unsupported expression type: " + expressionTypeToString(expression->expressionType));
    }
}

void UnaryOperatorExpressionEvaluator::evaluate() {
    children[0]->evaluate();
    execOperation(*children[0]->resultVector, *resultVector);
}

uint64_t UnaryOperatorExpressionEvaluator::select(sel_t* selectedPos) {
    children[0]->evaluate();
    return selectOperation(*children[0]->resultVector, selectedPos);
}

void BinaryOperatorExpressionEvaluator::init(
    const ResultSet& resultSet, MemoryManager* memoryManager) {
    OperatorExpressionEvaluator::init(resultSet, memoryManager);
    resultVector = make_shared<ValueVector>(memoryManager, expression->dataType);
    resultVector->state = !children[0]->isResultVectorFlat() ? children[0]->resultVector->state :
                                                               children[1]->resultVector->state;
}

void BinaryOperatorExpressionEvaluator::getExecOperation() {
    switch (expression->expressionType) {
    case ADD: {
        execOperation = VectorArithmeticOperations::Add;
    } break;
    case SUBTRACT: {
        execOperation = VectorArithmeticOperations::Subtract;
    } break;
    case MULTIPLY: {
        execOperation = VectorArithmeticOperations::Multiply;
    } break;
    case DIVIDE: {
        execOperation = VectorArithmeticOperations::Divide;
    } break;
    case MODULO: {
        execOperation = VectorArithmeticOperations::Modulo;
    } break;
    case POWER: {
        execOperation = VectorArithmeticOperations::Power;
    } break;
    default:
        throw invalid_argument(
            "Unsupported expression type: " + expressionTypeToString(expression->expressionType));
    }
}

void BinaryOperatorExpressionEvaluator::evaluate() {
    children[0]->evaluate();
    children[1]->evaluate();
    execOperation(*children[0]->resultVector, *children[1]->resultVector, *resultVector);
}

uint64_t BinaryOperatorExpressionEvaluator::select(sel_t* selectedPos) {
    children[0]->evaluate();
    children[1]->evaluate();
    return selectOperation(*children[0]->resultVector, *children[1]->resultVector, selectedPos);
}

} // namespace evaluator
} // namespace graphflow
