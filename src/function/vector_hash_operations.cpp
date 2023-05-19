#include "function/hash/vector_hash_operations.h"

#include "function/binary_operation_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void VectorHashOperations::computeHash(ValueVector* operand, ValueVector* result) {
    result->state = operand->state;
    assert(result->dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    switch (operand->dataType.getPhysicalType()) {
    case PhysicalTypeID::INTERNAL_ID: {
        UnaryHashOperationExecutor::execute<internalID_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::BOOL: {
        UnaryHashOperationExecutor::execute<bool, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::INT64: {
        UnaryHashOperationExecutor::execute<int64_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::INT32: {
        UnaryHashOperationExecutor::execute<int32_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::INT16: {
        UnaryHashOperationExecutor::execute<int16_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::DOUBLE: {
        UnaryHashOperationExecutor::execute<double, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::FLOAT: {
        UnaryHashOperationExecutor::execute<float_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::STRING: {
        UnaryHashOperationExecutor::execute<ku_string_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::INTERVAL: {
        UnaryHashOperationExecutor::execute<interval_t, hash_t>(*operand, *result);
    } break;
    default: {
        throw RuntimeException(
            "Cannot hash data type " +
            LogicalTypeUtils::dataTypeToString(operand->dataType.getLogicalTypeID()));
    }
    }
}

void VectorHashOperations::combineHash(ValueVector* left, ValueVector* right, ValueVector* result) {
    assert(left->dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    assert(left->dataType.getLogicalTypeID() == right->dataType.getLogicalTypeID());
    assert(left->dataType.getLogicalTypeID() == result->dataType.getLogicalTypeID());
    // TODO(Xiyang/Guodong): we should resolve result state of hash vector at compile time.
    result->state = !right->state->isFlat() ? right->state : left->state;
    BinaryOperationExecutor::execute<hash_t, hash_t, hash_t, operation::CombineHash>(
        *left, *right, *result);
}

} // namespace function
} // namespace kuzu
