#include "function/hash/vector_hash_functions.h"

#include "function/binary_function_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void VectorHashFunction::computeHash(ValueVector* operand, ValueVector* result) {
    result->state = operand->state;
    assert(result->dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    switch (operand->dataType.getPhysicalType()) {
    case PhysicalTypeID::INTERNAL_ID: {
        UnaryHashFunctionExecutor::execute<internalID_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::BOOL: {
        UnaryHashFunctionExecutor::execute<bool, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::INT64: {
        UnaryHashFunctionExecutor::execute<int64_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::INT32: {
        UnaryHashFunctionExecutor::execute<int32_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::INT16: {
        UnaryHashFunctionExecutor::execute<int16_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::INT8: {
        UnaryHashFunctionExecutor::execute<int8_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::UINT64: {
        UnaryHashFunctionExecutor::execute<uint64_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::UINT32: {
        UnaryHashFunctionExecutor::execute<uint32_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::UINT16: {
        UnaryHashFunctionExecutor::execute<uint16_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::UINT8: {
        UnaryHashFunctionExecutor::execute<uint8_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::DOUBLE: {
        UnaryHashFunctionExecutor::execute<double, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::FLOAT: {
        UnaryHashFunctionExecutor::execute<float_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::STRING: {
        UnaryHashFunctionExecutor::execute<ku_string_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::INTERVAL: {
        UnaryHashFunctionExecutor::execute<interval_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::STRUCT: {
        if (operand->dataType.getLogicalTypeID() == LogicalTypeID::NODE) {
            assert(0 == common::StructType::getFieldIdx(&operand->dataType, InternalKeyword::ID));
            UnaryHashFunctionExecutor::execute<internalID_t, hash_t>(
                *StructVector::getFieldVector(operand, 0), *result);
            break;
        } else if (operand->dataType.getLogicalTypeID() == LogicalTypeID::REL) {
            assert(3 == StructType::getFieldIdx(&operand->dataType, InternalKeyword::ID));
            UnaryHashFunctionExecutor::execute<internalID_t, hash_t>(
                *StructVector::getFieldVector(operand, 3), *result);
            break;
        }
    }
    default: {
        throw RuntimeException(
            "Cannot hash data type " +
            LogicalTypeUtils::dataTypeToString(operand->dataType.getLogicalTypeID()));
    }
    }
}

void VectorHashFunction::combineHash(ValueVector* left, ValueVector* right, ValueVector* result) {
    assert(left->dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    assert(left->dataType.getLogicalTypeID() == right->dataType.getLogicalTypeID());
    assert(left->dataType.getLogicalTypeID() == result->dataType.getLogicalTypeID());
    // TODO(Xiyang/Guodong): we should resolve result state of hash vector at compile time.
    result->state = !right->state->isFlat() ? right->state : left->state;
    BinaryFunctionExecutor::execute<hash_t, hash_t, hash_t, CombineHash>(*left, *right, *result);
}

} // namespace function
} // namespace kuzu
