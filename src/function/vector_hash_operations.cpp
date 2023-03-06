#include "function/hash/vector_hash_operations.h"

#include "function/binary_operation_executor.h"
#include "function/unary_operation_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void VectorHashOperations::computeHash(ValueVector* operand, ValueVector* result) {
    result->state = operand->state;
    assert(result->dataType.typeID == INT64);
    switch (operand->dataType.typeID) {
    case INTERNAL_ID: {
        UnaryHashOperationExecutor::execute<internalID_t, hash_t>(*operand, *result);
    } break;
    case BOOL: {
        UnaryHashOperationExecutor::execute<bool, hash_t>(*operand, *result);
    } break;
    case INT64: {
        UnaryHashOperationExecutor::execute<int64_t, hash_t>(*operand, *result);
    } break;
    case INT32: {
        UnaryHashOperationExecutor::execute<int32_t, hash_t>(*operand, *result);
    } break;
    case INT16: {
        UnaryHashOperationExecutor::execute<int16_t, hash_t>(*operand, *result);
    } break;
    case DOUBLE: {
        UnaryHashOperationExecutor::execute<double, hash_t>(*operand, *result);
    } break;
    case FLOAT: {
        UnaryHashOperationExecutor::execute<float_t, hash_t>(*operand, *result);
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
    // TODO(Xiyang/Guodong): we should resolve result state of hash vector at compile time.
    result->state = !right->state->isFlat() ? right->state : left->state;
    BinaryOperationExecutor::execute<hash_t, hash_t, hash_t, operation::CombineHash>(
        *left, *right, *result);
}

} // namespace function
} // namespace kuzu
