#include "src/common/include/vector/operations/vector_hash_operations.h"

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

struct VectorHashExecutor {
    template<class OP>
    static inline void execute(ValueVector& operand, ValueVector& result) {
        switch (operand.dataType) {
        case INT64:
            UnaryOperationExecutor::executeHashOps<int64_t, OP>(operand, result);
            break;
        case DOUBLE:
            UnaryOperationExecutor::executeHashOps<double_t, OP>(operand, result);
            break;
        default:
            assert(false);
        }
    }
};

void VectorHashOperations::Hash(ValueVector& operand, ValueVector& result) {
    assert(operand.dataType != UNSTRUCTURED); // Unstructured not supported yet.
    VectorHashExecutor::execute<operation::Hash>(operand, result);
}
} // namespace common
} // namespace graphflow
