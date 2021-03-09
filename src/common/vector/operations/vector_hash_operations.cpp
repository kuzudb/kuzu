#include "src/common/include/vector/operations/vector_hash_operations.h"

#include <stdexcept>

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

struct HashNodeIDOperationExecutor {
public:
    template<class OP>
    static inline void execute(ValueVector& operand, ValueVector& result) {
        auto& nodeIdOperand = dynamic_cast<NodeIDVector&>(operand);
        switch (nodeIdOperand.getCompressionScheme().getNumBytesForOffset()) {
        case 2:
            UnaryOperationExecutor::executeOnNodeIDVector<uint16_t, uint64_t, OP>(
                nodeIdOperand, result);
            break;
        case 4:
            UnaryOperationExecutor::executeOnNodeIDVector<uint32_t, uint64_t, OP>(
                nodeIdOperand, result);
            break;
        case 8:
            UnaryOperationExecutor::executeOnNodeIDVector<uint64_t, uint64_t, OP>(
                nodeIdOperand, result);
            break;
        default:
            throw std::invalid_argument("Invalid or unsupported operand type for hash operation.");
        }
    }
};

void VectorHashOperations::HashNodeID(ValueVector& operand, ValueVector& result) {
    HashNodeIDOperationExecutor::execute<operation::HashNodeID>(operand, result);
}

} // namespace common
} // namespace graphflow
