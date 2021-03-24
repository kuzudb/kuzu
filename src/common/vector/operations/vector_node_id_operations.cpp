#include "src/common/include/vector/operations/vector_node_id_operations.h"

#include "src/common/include/operations/comparison_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"

namespace graphflow {
namespace common {

void VectorNodeIDCompareOperations::Equals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeOnNodeIDs<operation::Equals>(left, right, result);
}

void VectorNodeIDCompareOperations::NotEquals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeOnNodeIDs<operation::NotEquals>(left, right, result);
}

void VectorNodeIDCompareOperations::GreaterThan(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeOnNodeIDs<operation::GreaterThan>(left, right, result);
}

void VectorNodeIDCompareOperations::GreaterThanEquals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeOnNodeIDs<operation::GreaterThanEquals>(left, right, result);
}

void VectorNodeIDCompareOperations::LessThan(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeOnNodeIDs<operation::LessThan>(left, right, result);
}

void VectorNodeIDCompareOperations::LessThanEquals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeOnNodeIDs<operation::LessThanEquals>(left, right, result);
}

} // namespace common
} // namespace graphflow
