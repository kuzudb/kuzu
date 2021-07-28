#include "src/common/include/vector/operations/vector_node_id_operations.h"

#include "src/common/include/operations/comparison_operations.h"
#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

void VectorNodeIDOperations::Equals(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeNodeIDOps<operation::Equals>(left, right, result);
}

void VectorNodeIDOperations::NotEquals(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeNodeIDOps<operation::NotEquals>(left, right, result);
}

void VectorNodeIDOperations::GreaterThan(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeNodeIDOps<operation::GreaterThan>(left, right, result);
}

void VectorNodeIDOperations::GreaterThanEquals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeNodeIDOps<operation::GreaterThanEquals>(left, right, result);
}

void VectorNodeIDOperations::LessThan(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeNodeIDOps<operation::LessThan>(left, right, result);
}

void VectorNodeIDOperations::LessThanEquals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeNodeIDOps<operation::LessThanEquals>(left, right, result);
}

void VectorNodeIDOperations::Hash(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::executeNodeIDOps<uint64_t, operation::Hash>(operand, result);
}

uint64_t VectorNodeIDOperations::EqualsSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::selectNodeIDOps<operation::Equals>(
        left, right, selectedPositions);
}

uint64_t VectorNodeIDOperations::NotEqualsSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::selectNodeIDOps<operation::NotEquals>(
        left, right, selectedPositions);
}

uint64_t VectorNodeIDOperations::GreaterThanSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::selectNodeIDOps<operation::GreaterThan>(
        left, right, selectedPositions);
}

uint64_t VectorNodeIDOperations::GreaterThanEqualsSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::selectNodeIDOps<operation::GreaterThanEquals>(
        left, right, selectedPositions);
}

uint64_t VectorNodeIDOperations::LessThanSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::selectNodeIDOps<operation::LessThan>(
        left, right, selectedPositions);
}

uint64_t VectorNodeIDOperations::LessThanEqualsSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return BinaryOperationExecutor::selectNodeIDOps<operation::LessThanEquals>(
        left, right, selectedPositions);
}

} // namespace common
} // namespace graphflow
