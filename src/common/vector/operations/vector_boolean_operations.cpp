#include "src/common/include/vector/operations/vector_boolean_operations.h"

#include "src/common/include/operations/boolean_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

void VectorBooleanOperations::And(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::execute<bool, bool, uint8_t, operation::And, true /* IS_BOOL_OP */>(
        left, right, result);
}

void VectorBooleanOperations::Or(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::execute<bool, bool, uint8_t, operation::Or, true /* IS_BOOL_OP */>(
        left, right, result);
}

void VectorBooleanOperations::Xor(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::execute<bool, bool, uint8_t, operation::Xor, true /* IS_BOOL_OP */>(
        left, right, result);
}

void VectorBooleanOperations::Not(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::execute<bool, uint8_t, operation::Not, true /* IS_BOOL_OP */>(
        operand, result);
}

uint64_t VectorBooleanOperations::AndSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::select<bool, bool, uint8_t, operation::And,
        true /* IS_BOOL_OP */>(left, right, selectedPositions);
}

uint64_t VectorBooleanOperations::OrSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::select<bool, bool, uint8_t, operation::Or,
        true /* IS_BOOL_OP */>(left, right, selectedPositions);
}

uint64_t VectorBooleanOperations::XorSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::select<bool, bool, uint8_t, operation::Xor,
        true /* IS_BOOL_OP */>(left, right, selectedPositions);
}

uint64_t VectorBooleanOperations::NotSelect(ValueVector& operand, sel_t* selectedPositions) {
    return UnaryOperationExecutor::select<operation::Not>(operand, selectedPositions);
}

} // namespace common
} // namespace graphflow
