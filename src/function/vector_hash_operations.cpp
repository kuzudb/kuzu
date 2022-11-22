#include "function/hash/vector_hash_operations.h"

#include "function/binary_operation_executor.h"
#include "function/unary_operation_executor.h"

namespace kuzu {
namespace function {

void VectorHashOperations::computeHash(ValueVector* operand, ValueVector* result) {
    assert(result->dataType.typeID == INT64);
    switch (operand->dataType.typeID) {
    case NODE_ID: {
        UnaryHashOperationExecutor::execute<nodeID_t, hash_t>(*operand, *result);
    } break;
    case BOOL: {
        UnaryHashOperationExecutor::execute<bool, hash_t>(*operand, *result);
    } break;
    case INT64: {
        UnaryHashOperationExecutor::execute<int64_t, hash_t>(*operand, *result);
    } break;
    case DOUBLE: {
        UnaryHashOperationExecutor::execute<double, hash_t>(*operand, *result);
    } break;
    case STRING: {
        UnaryHashOperationExecutor::execute<ku_string_t, hash_t>(*operand, *result);
    } break;
    case DATE: {
        UnaryHashOperationExecutor::execute<date_t, hash_t>(*operand, *result);
    } break;
    case TIMESTAMP: {
        UnaryHashOperationExecutor::execute<timestamp_t, hash_t>(*operand, *result);
    } break;
    case INTERVAL: {
        UnaryHashOperationExecutor::execute<interval_t, hash_t>(*operand, *result);
    } break;
    default: {
        throw RuntimeException(
            "Cannot hash data type " + Types::dataTypeToString(operand->dataType.typeID));
    }
    }
}

void VectorHashOperations::combineHash(ValueVector* left, ValueVector* right, ValueVector* result) {
    assert(left->dataType.typeID == INT64);
    assert(left->dataType.typeID == right->dataType.typeID);
    assert(left->dataType.typeID == result->dataType.typeID);
    BinaryOperationExecutor::execute<hash_t, hash_t, hash_t, operation::CombineHash>(
        *left, *right, *result);
}

} // namespace function
} // namespace kuzu
