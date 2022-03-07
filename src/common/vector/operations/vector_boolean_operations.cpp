#include "src/common/include/vector/operations/vector_boolean_operations.h"

#include "src/common/include/operations/boolean_operations.h"
#include "src/common/include/vector/operations/executors/boolean_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

void VectorBooleanOperations::And(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryBooleanOperationExecutor::execute<operation::And>(left, right, result);
}

void VectorBooleanOperations::Or(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryBooleanOperationExecutor::execute<operation::Or>(left, right, result);
}

void VectorBooleanOperations::Xor(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryBooleanOperationExecutor::execute<operation::Xor>(left, right, result);
}

void VectorBooleanOperations::Not(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::execute<bool, uint8_t, operation::Not>(operand, result);
}

uint64_t VectorBooleanOperations::AndSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryBooleanOperationExecutor::select<operation::And>(left, right, selectedPositions);
}

uint64_t VectorBooleanOperations::OrSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryBooleanOperationExecutor::select<operation::Or>(left, right, selectedPositions);
}

uint64_t VectorBooleanOperations::XorSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryBooleanOperationExecutor::select<operation::Xor>(left, right, selectedPositions);
}

uint64_t VectorBooleanOperations::NotSelect(ValueVector& operand, sel_t* selectedPositions) {
    return UnaryOperationExecutor::select<operation::Not>(operand, selectedPositions);
}

} // namespace common
} // namespace graphflow
