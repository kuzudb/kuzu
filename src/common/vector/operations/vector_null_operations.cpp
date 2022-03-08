#include "src/common/include/vector/operations/vector_null_operations.h"

#include "src/common/include/operations/null_operations.h"
#include "src/common/include/vector/operations/executors/null_operation_executor.h"

namespace graphflow {
namespace common {

void VectorNullOperations::IsNull(ValueVector& operand, ValueVector& result) {
    NullOperationExecutor::execute<operation::IsNull>(operand, result);
}

void VectorNullOperations::IsNotNull(ValueVector& operand, ValueVector& result) {
    NullOperationExecutor::execute<operation::IsNotNull>(operand, result);
}

uint64_t VectorNullOperations::IsNullSelect(ValueVector& operand, sel_t* selectedPositions) {
    return NullOperationExecutor::select<operation::IsNull>(operand, selectedPositions);
}

uint64_t VectorNullOperations::IsNotNullSelect(ValueVector& operand, sel_t* selectedPositions) {
    return NullOperationExecutor::select<operation::IsNotNull>(operand, selectedPositions);
}

} // namespace common
} // namespace graphflow
