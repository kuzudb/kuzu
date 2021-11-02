#include "src/common/include/vector/operations/vector_boolean_operations.h"

#include "src/common/include/operations/boolean_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

void VectorBooleanOperations::And(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::execute<uint8_t, uint8_t, uint8_t, operation::And,
        false /* SKIP_NULL */>(left, right, result);
}

void VectorBooleanOperations::Or(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::execute<uint8_t, uint8_t, uint8_t, operation::Or,
        false /* SKIP_NULL */>(left, right, result);
}

void VectorBooleanOperations::Xor(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::execute<uint8_t, uint8_t, uint8_t, operation::Xor,
        false /* SKIP_NULL */>(left, right, result);
}

void VectorBooleanOperations::Not(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::execute<uint8_t, uint8_t, operation::Not, false /* SKIP_NULL */>(
        operand, result);
}

uint64_t VectorBooleanOperations::AndSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::select<uint8_t, uint8_t, uint8_t, operation::And,
        false /* SKIP_NULL */>(left, right, selectedPositions);
}

uint64_t VectorBooleanOperations::OrSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::select<uint8_t, uint8_t, uint8_t, operation::Or,
        false /* SKIP_NULL */>(left, right, selectedPositions);
}

uint64_t VectorBooleanOperations::XorSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::select<uint8_t, uint8_t, uint8_t, operation::Xor,
        false /* SKIP_NULL */>(left, right, selectedPositions);
}

uint64_t VectorBooleanOperations::NotSelect(ValueVector& operand, sel_t* selectedPositions) {
    return UnaryOperationExecutor::select<operation::Not>(operand, selectedPositions);
}

} // namespace common
} // namespace graphflow
