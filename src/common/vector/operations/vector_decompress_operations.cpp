#include "src/common/include/vector/operations/vector_decompress_operations.h"

#include <stdexcept>

#include "src/common/include/operations/decompress_operations.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

void VectorDecompressOperations::DecompressNodeID(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::executeOnNodeIDVector<nodeID_t, operation::DecompressNodeID>(
        operand, result);
}

} // namespace common
} // namespace graphflow
