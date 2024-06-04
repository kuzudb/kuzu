#include "function/hash/vector_hash_functions.h"

#include "function/binary_function_executor.h"
#include "function/hash/hash_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename OPERAND_TYPE, typename RESULT_TYPE>
void UnaryHashFunctionExecutor::execute(ValueVector& operand, SelectionVector& operandSelectVec,
    ValueVector& result, SelectionVector& resultSelectVec) {
    auto resultValues = (RESULT_TYPE*)result.getData();
    if (operand.hasNoNullsGuarantee()) {
        if (operandSelectVec.isUnfiltered()) {
            for (auto i = 0u; i < operandSelectVec.getSelSize(); i++) {
                auto resultPos = resultSelectVec[i];
                Hash::operation(operand.getValue<OPERAND_TYPE>(i), resultValues[resultPos],
                    &operand);
            }
        } else {
            for (auto i = 0u; i < operandSelectVec.getSelSize(); i++) {
                auto operandPos = operandSelectVec[i];
                auto resultPos = resultSelectVec[i];
                Hash::operation(operand.getValue<OPERAND_TYPE>(operandPos), resultValues[resultPos],
                    &operand);
            }
        }
    } else {
        if (operandSelectVec.isUnfiltered()) {
            for (auto i = 0u; i < operandSelectVec.getSelSize(); i++) {
                auto resultPos = resultSelectVec[i];
                if (!operand.isNull(i)) {
                    Hash::operation(operand.getValue<OPERAND_TYPE>(i), resultValues[resultPos],
                        &operand);
                } else {
                    result.setValue(resultPos, NULL_HASH);
                }
            }
        } else {
            for (auto i = 0u; i < operandSelectVec.getSelSize(); i++) {
                auto operandPos = operandSelectVec[i];
                auto resultPos = resultSelectVec[i];
                if (!operand.isNull(operandPos)) {
                    Hash::operation(operand.getValue<OPERAND_TYPE>(operandPos),
                        resultValues[resultPos], &operand);
                } else {
                    result.setValue(resultPos, NULL_HASH);
                }
            }
        }
    }
}

static std::unique_ptr<ValueVector> computeDataVecHash(ValueVector& operand) {
    auto hashVector = std::make_unique<ValueVector>(*LogicalType::LIST(LogicalType::HASH()));
    auto numValuesInDataVec = ListVector::getDataVectorSize(&operand);
    ListVector::resizeDataVector(hashVector.get(), numValuesInDataVec);
    // TODO(Ziyi): Allow selection size to be greater than default vector capacity, so we don't have
    // to chunk the selectionVector.
    SelectionVector selectionVector{DEFAULT_VECTOR_CAPACITY};
    selectionVector.setToFiltered();
    auto numValuesComputed = 0u;
    while (numValuesComputed < numValuesInDataVec) {
        for (auto i = 0u; i < DEFAULT_VECTOR_CAPACITY; i++) {
            selectionVector[i] = numValuesComputed;
            numValuesComputed++;
        }
        VectorHashFunction::computeHash(*ListVector::getDataVector(&operand), selectionVector,
            *ListVector::getDataVector(hashVector.get()), selectionVector);
    }
    return hashVector;
}

static void finalizeDataVecHash(ValueVector& operand, SelectionVector& operandSelVec,
    ValueVector& result, ValueVector& hashVec) {
    for (auto i = 0u; i < operandSelVec.getSelSize(); i++) {
        auto pos = operand.state->getSelVector()[i];
        auto entry = operand.getValue<list_entry_t>(pos);
        if (operand.isNull(pos)) {
            result.setValue(pos, NULL_HASH);
        } else {
            auto hashValue = NULL_HASH;
            for (auto j = 0u; j < entry.size; j++) {
                hashValue = combineHashScalar(hashValue,
                    ListVector::getDataVector(&hashVec)->getValue<hash_t>(entry.offset + j));
            }
            result.setValue(pos, hashValue);
        }
    }
}

static void computeListVectorHash(ValueVector& operand, SelectionVector& operandSelectVec,
    ValueVector& result, SelectionVector& resultSelectVec) {
    // TODO(Ziyi): fix
    auto dataVecHash = computeDataVecHash(operand);
    finalizeDataVecHash(operand, operandSelectVec, result, *dataVecHash);
}

static void computeStructVecHash(ValueVector& operand, SelectionVector& operandSelectVec,
    ValueVector& result, SelectionVector& resultSelectVec) {
    switch (operand.dataType.getLogicalTypeID()) {
    case LogicalTypeID::NODE: {
        KU_ASSERT(0 == common::StructType::getFieldIdx(operand.dataType, InternalKeyword::ID));
        UnaryHashFunctionExecutor::execute<internalID_t, hash_t>(
            *StructVector::getFieldVector(&operand, 0), operandSelectVec, result, resultSelectVec);
    } break;
    case LogicalTypeID::REL: {
        KU_ASSERT(3 == StructType::getFieldIdx(operand.dataType, InternalKeyword::ID));
        UnaryHashFunctionExecutor::execute<internalID_t, hash_t>(
            *StructVector::getFieldVector(&operand, 3), operandSelectVec, result, resultSelectVec);
    } break;
    case LogicalTypeID::STRUCT: {
        result.state = operand.state;
        VectorHashFunction::computeHash(*StructVector::getFieldVector(&operand, 0 /* idx */),
            operandSelectVec, result, resultSelectVec);
        auto tmpHashVector = std::make_unique<ValueVector>(LogicalTypeID::INT64);
        for (auto i = 1u; i < StructType::getNumFields(operand.dataType); i++) {
            auto fieldVector = StructVector::getFieldVector(&operand, i);
            VectorHashFunction::computeHash(*fieldVector, operandSelectVec, *tmpHashVector,
                resultSelectVec);
            VectorHashFunction::combineHash(*tmpHashVector, resultSelectVec, result,
                resultSelectVec, result, resultSelectVec);
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void VectorHashFunction::computeHash(ValueVector& operand, SelectionVector& operandSelectVec,
    ValueVector& result, SelectionVector& resultSelectVec) {
    result.state = operand.state;
    KU_ASSERT(result.dataType.getLogicalTypeID() == LogicalType::HASH()->getLogicalTypeID());
    TypeUtils::visit(
        operand.dataType.getPhysicalType(),
        [&]<HashableNonNestedTypes T>(T) {
            UnaryHashFunctionExecutor::execute<T, hash_t>(operand, operandSelectVec, result,
                resultSelectVec);
        },
        [&](struct_entry_t) {
            computeStructVecHash(operand, operandSelectVec, result, resultSelectVec);
        },
        [&](list_entry_t) {
            computeListVectorHash(operand, operandSelectVec, result, resultSelectVec);
        },
        [&operand](auto) {
            // LCOV_EXCL_START
            throw RuntimeException("Cannot hash data type " + operand.dataType.toString());
            // LCOV_EXCL_STOP
        });
}

void VectorHashFunction::combineHash(ValueVector* left, ValueVector* right, ValueVector* result) {
    KU_ASSERT(left->dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    KU_ASSERT(left->dataType.getLogicalTypeID() == right->dataType.getLogicalTypeID());
    KU_ASSERT(left->dataType.getLogicalTypeID() == result->dataType.getLogicalTypeID());
    // TODO(Xiyang/Guodong): we should resolve result state of hash vector at compile time.
    result->state = !right->state->isFlat() ? right->state : left->state;
    BinaryFunctionExecutor::execute<hash_t, hash_t, hash_t, CombineHash>(*left, *right, *result);
}

void VectorHashFunction::combineHash(ValueVector& left, SelectionVector& leftSelVec,
    ValueVector& right, SelectionVector& rightSelVec, ValueVector& result,
    SelectionVector& resultSelVec) {
    KU_ASSERT(left.dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    KU_ASSERT(left.dataType.getLogicalTypeID() == right.dataType.getLogicalTypeID());
    KU_ASSERT(left.dataType.getLogicalTypeID() == result.dataType.getLogicalTypeID());
    BinaryHashFunctionExecutor::execute<hash_t, hash_t, hash_t, CombineHash>(left, leftSelVec,
        right, rightSelVec, result, resultSelVec);
}

static void HashExecFunc(const std::vector<std::shared_ptr<common::ValueVector>>& params,
    common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
    KU_ASSERT(params.size() == 1);
    // Todo(ziyi): revisit this code.
    result.state = params[0]->state;
    VectorHashFunction::computeHash(*params[0], params[0]->state->getSelVectorUnsafe(), result,
        result.state->getSelVectorUnsafe());
}

function_set HashFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::INT64, HashExecFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
