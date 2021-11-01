#include "src/common/include/vector/operations/vector_hash_operations.h"

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

void VectorHashOperations::Hash(ValueVector& operand, ValueVector& result) {
    switch (operand.dataType) {
    case NODE: {
        UnaryOperationExecutor::execute<nodeID_t, hash_t, operation::Hash, true /* IS_NODE_ID */,
            true /* SKIP_NULL */>(operand, result);
    } break;
    case INT64: {
        UnaryOperationExecutor::execute<int64_t, hash_t, operation::Hash, false /* IS_NODE_ID */,
            true /* SKIP_NULL */>(operand, result);
    } break;
    case DOUBLE: {
        UnaryOperationExecutor::execute<double_t, hash_t, operation::Hash, false /* IS_NODE_ID */,
            true /* SKIP_NULL */>(operand, result);
    } break;
    case STRING: {
        UnaryOperationExecutor::execute<gf_string_t, hash_t, operation::Hash,
            false /* IS_NODE_ID */, true /* SKIP_NULL */>(operand, result);
    } break;
    case DATE: {
        UnaryOperationExecutor::execute<date_t, hash_t, operation::Hash, false /* IS_NODE_ID */,
            true /* SKIP_NULL */>(operand, result);
    } break;
    case TIMESTAMP: {
        UnaryOperationExecutor::execute<timestamp_t, hash_t, operation::Hash,
            false /* IS_NODE_ID */, true /* SKIP_NULL */>(operand, result);
    } break;
    case INTERVAL: {
        UnaryOperationExecutor::execute<interval_t, hash_t, operation::Hash, false /* IS_NODE_ID */,
            true /* SKIP_NULL */>(operand, result);
    } break;
    default:
        assert(false);
    }
}

void VectorHashOperations::CombineHash(ValueVector& operand, ValueVector& result) {
    switch (operand.dataType) {
    case NODE: {
        UnaryOperationExecutor::execute<nodeID_t, hash_t, operation::CombineHash,
            true /* IS_NODE_ID */, true /* SKIP_NULL */>(operand, result);
    } break;
    case INT64: {
        UnaryOperationExecutor::execute<int64_t, hash_t, operation::CombineHash,
            false /* IS_NODE_ID */, true /* SKIP_NULL */>(operand, result);
    } break;
    case DOUBLE: {
        UnaryOperationExecutor::execute<double_t, hash_t, operation::CombineHash,
            false /* IS_NODE_ID */, true /* SKIP_NULL */>(operand, result);
    } break;
    case STRING: {
        UnaryOperationExecutor::execute<gf_string_t, hash_t, operation::CombineHash,
            false /* IS_NODE_ID */, true /* SKIP_NULL */>(operand, result);
    } break;
    case DATE: {
        UnaryOperationExecutor::execute<date_t, hash_t, operation::CombineHash,
            false /* IS_NODE_ID */, true /* SKIP_NULL */>(operand, result);
    } break;
    case TIMESTAMP: {
        UnaryOperationExecutor::execute<timestamp_t, hash_t, operation::CombineHash,
            false /* IS_NODE_ID */, true /* SKIP_NULL */>(operand, result);
    } break;
    case INTERVAL: {
        UnaryOperationExecutor::execute<interval_t, hash_t, operation::CombineHash,
            false /* IS_NODE_ID */, true /* SKIP_NULL */>(operand, result);
    } break;
    default:
        assert(false);
    }
}
} // namespace common
} // namespace graphflow
