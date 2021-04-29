#include "src/common/include/vector/operations/vector_boolean_operations.h"

#include "src/common/include/operations/boolean_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

void VectorBooleanOperations::And(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeBoolOps<operation::And>(left, right, result);
}

void VectorBooleanOperations::Or(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeBoolOps<operation::Or>(left, right, result);
}

void VectorBooleanOperations::Xor(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeBoolOps<operation::Xor>(left, right, result);
}

void VectorBooleanOperations::Not(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::executeBoolOperation<operation::Not>(operand, result);
}

} // namespace common
} // namespace graphflow
