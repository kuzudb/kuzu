#include "src/common/include/vector/operations/vector_hash_operations.h"

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

void VectorHashOperations::Hash(ValueVector& operand, ValueVector& result) {
    switch (operand.dataType) {
    case INT64:
        UnaryOperationExecutor::execute<int64_t, uint64_t, operation::Hash>(operand, result);
        break;
    case DOUBLE:
        UnaryOperationExecutor::execute<double_t, uint64_t, operation::Hash>(operand, result);
        break;
    default:
        assert(false);
    }
}
} // namespace common
} // namespace graphflow
