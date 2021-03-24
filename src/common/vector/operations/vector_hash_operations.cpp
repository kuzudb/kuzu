#include "src/common/include/vector/operations/vector_hash_operations.h"

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

void VectorHashOperations::HashNodeID(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::executeOnNodeIDVector<uint64_t, operation::Hash>(operand, result);
}

} // namespace common
} // namespace graphflow
