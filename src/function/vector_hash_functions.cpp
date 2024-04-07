#include "function/hash/vector_hash_functions.h"

#include "function/binary_function_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<ValueVector> computeDataVecHash(ValueVector* operand) {
    auto hashVector = std::make_unique<ValueVector>(*LogicalType::LIST(LogicalType::HASH()));
    auto numValuesInDataVec = ListVector::getDataVectorSize(operand);
    ListVector::resizeDataVector(hashVector.get(), numValuesInDataVec);
    auto selectionState = std::make_shared<DataChunkState>();
    selectionState->selVector->setToFiltered();
    ListVector::getDataVector(operand)->setState(selectionState);
    auto numValuesComputed = 0u;
    while (numValuesComputed < numValuesInDataVec) {
        for (auto i = 0u; i < DEFAULT_VECTOR_CAPACITY; i++) {
            selectionState->selVector->selectedPositions[i] = numValuesComputed;
            numValuesComputed++;
        }
        VectorHashFunction::computeHash(ListVector::getDataVector(operand),
            ListVector::getDataVector(hashVector.get()));
    }
    return hashVector;
}

static void finalizeDataVecHash(ValueVector* operand, ValueVector* result, ValueVector* hashVec) {
    for (auto i = 0u; i < result->state->getNumSelectedValues(); i++) {
        auto pos = operand->state->selVector->selectedPositions[i];
        auto entry = operand->getValue<list_entry_t>(pos);
        if (operand->isNull(pos)) {
            result->setValue(pos, NULL_HASH);
        } else {
            auto hashValue = NULL_HASH;
            for (auto j = 0u; j < entry.size; j++) {
                hashValue = combineHashScalar(hashValue,
                    ListVector::getDataVector(hashVec)->getValue<hash_t>(entry.offset + j));
            }
            result->setValue(pos, hashValue);
        }
    }
}

static void computeListVectorHash(ValueVector* operand, ValueVector* result) {
    auto dataVecHash = computeDataVecHash(operand);
    finalizeDataVecHash(operand, result, dataVecHash.get());
}

static void computeStructVecHash(ValueVector* operand, ValueVector* result) {
    switch (operand->dataType.getLogicalTypeID()) {
    case LogicalTypeID::NODE: {
        KU_ASSERT(0 == common::StructType::getFieldIdx(&operand->dataType, InternalKeyword::ID));
        UnaryHashFunctionExecutor::execute<internalID_t, hash_t>(
            *StructVector::getFieldVector(operand, 0), *result);
    } break;
    case LogicalTypeID::REL: {
        KU_ASSERT(3 == StructType::getFieldIdx(&operand->dataType, InternalKeyword::ID));
        UnaryHashFunctionExecutor::execute<internalID_t, hash_t>(
            *StructVector::getFieldVector(operand, 3), *result);
    } break;
    case LogicalTypeID::STRUCT: {
        VectorHashFunction::computeHash(StructVector::getFieldVector(operand, 0 /* idx */).get(),
            result);
        auto tmpHashVector = std::make_unique<ValueVector>(LogicalTypeID::INT64);
        for (auto i = 1u; i < StructType::getNumFields(&operand->dataType); i++) {
            auto fieldVector = StructVector::getFieldVector(operand, i);
            VectorHashFunction::computeHash(fieldVector.get(), tmpHashVector.get());
            VectorHashFunction::combineHash(tmpHashVector.get(), result, result);
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void VectorHashFunction::computeHash(ValueVector* operand, ValueVector* result) {
    result->state = operand->state;
    KU_ASSERT(result->dataType.getLogicalTypeID() == LogicalTypeID::INT64);
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
    case PhysicalTypeID::INT128: {
        UnaryHashFunctionExecutor::execute<int128_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::DOUBLE: {
        UnaryHashFunctionExecutor::execute<double, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::FLOAT: {
        UnaryHashFunctionExecutor::execute<float, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::STRING: {
        UnaryHashFunctionExecutor::execute<ku_string_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::INTERVAL: {
        UnaryHashFunctionExecutor::execute<interval_t, hash_t>(*operand, *result);
    } break;
    case PhysicalTypeID::STRUCT: {
        computeStructVecHash(operand, result);
    } break;
    case PhysicalTypeID::LIST: {
        computeListVectorHash(operand, result);
    } break;
        // LCOV_EXCL_START
    default: {
        throw RuntimeException("Cannot hash data type " + operand->dataType.toString());
    }
        // LCOV_EXCL_STOP
    }
}

void VectorHashFunction::combineHash(ValueVector* left, ValueVector* right, ValueVector* result) {
    KU_ASSERT(left->dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    KU_ASSERT(left->dataType.getLogicalTypeID() == right->dataType.getLogicalTypeID());
    KU_ASSERT(left->dataType.getLogicalTypeID() == result->dataType.getLogicalTypeID());
    // TODO(Xiyang/Guodong): we should resolve result state of hash vector at compile time.
    result->state = !right->state->isFlat() ? right->state : left->state;
    BinaryFunctionExecutor::execute<hash_t, hash_t, hash_t, CombineHash>(*left, *right, *result);
}

} // namespace function
} // namespace kuzu
