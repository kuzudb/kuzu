#include "processor/result/base_hash_table.h"

#include "math.h"

#include "common/null_buffer.h"
#include "common/type_utils.h"
#include "common/utils.h"
#include "function/comparison/comparison_functions.h"
#include "function/hash/vector_hash_functions.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

BaseHashTable::BaseHashTable(storage::MemoryManager& memoryManager, logical_type_vec_t keyTypes)
    : maxNumHashSlots{0}, bitmask{0}, numSlotsPerBlockLog2{0}, slotIdxInBlockMask{0},
      memoryManager{memoryManager}, keyTypes{std::move(keyTypes)} {
    initCompareFuncs();
    initTmpHashVector();
}

void BaseHashTable::setMaxNumHashSlots(uint64_t newSize) {
    maxNumHashSlots = newSize;
    bitmask = maxNumHashSlots - 1;
}

void BaseHashTable::computeAndCombineVecHash(const std::vector<ValueVector*>& unFlatKeyVectors,
    uint32_t startVecIdx) {
    for (; startVecIdx < unFlatKeyVectors.size(); startVecIdx++) {
        auto keyVector = unFlatKeyVectors[startVecIdx];
        auto tmpHashResultVector =
            std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
        auto tmpHashCombineResultVector =
            std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
        VectorHashFunction::computeHash(keyVector, tmpHashResultVector.get());
        VectorHashFunction::combineHash(hashVector.get(), tmpHashResultVector.get(),
            tmpHashCombineResultVector.get());
        hashVector = std::move(tmpHashCombineResultVector);
    }
}

void BaseHashTable::computeVectorHashes(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors) {
    if (!flatKeyVectors.empty()) {
        VectorHashFunction::computeHash(flatKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(flatKeyVectors, 1 /* startVecIdx */);
        computeAndCombineVecHash(unFlatKeyVectors, 0 /* startVecIdx */);
    } else {
        VectorHashFunction::computeHash(unFlatKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(unFlatKeyVectors, 1 /* startVecIdx */);
    }
}

template<typename T>
static bool compareEntry(common::ValueVector* vector, uint32_t vectorPos, const uint8_t* entry) {
    uint8_t result;
    auto key = vector->getData() + vectorPos * vector->getNumBytesPerValue();
    function::Equals::operation(*(T*)key, *(T*)entry, result, nullptr /* leftVector */,
        nullptr /* rightVector */);
    return result != 0;
}

static compare_function_t getCompareEntryFunc(PhysicalTypeID type);

template<>
[[maybe_unused]] bool compareEntry<list_entry_t>(common::ValueVector* vector, uint32_t vectorPos,
    const uint8_t* entry) {
    auto dataVector = ListVector::getDataVector(vector);
    auto listToCompare = vector->getValue<list_entry_t>(vectorPos);
    auto listEntry = reinterpret_cast<const ku_list_t*>(entry);
    auto entryNullBytes = reinterpret_cast<uint8_t*>(listEntry->overflowPtr);
    auto entryValues = entryNullBytes + NullBuffer::getNumBytesForNullValues(listEntry->size);
    auto rowLayoutSize = LogicalTypeUtils::getRowLayoutSize(dataVector->dataType);
    compare_function_t compareFunc = getCompareEntryFunc(dataVector->dataType.getPhysicalType());
    if (listToCompare.size != listEntry->size) {
        return false;
    }
    for (auto i = 0u; i < listEntry->size; i++) {
        if (!compareFunc(dataVector, listToCompare.offset + i, entryValues)) {
            return false;
        }
        entryValues += rowLayoutSize;
    }
    return true;
}

static bool compareNodeEntry(common::ValueVector* vector, uint32_t vectorPos,
    const uint8_t* entry) {
    KU_ASSERT(0 == common::StructType::getFieldIdx(vector->dataType, common::InternalKeyword::ID));
    auto idVector = common::StructVector::getFieldVector(vector, 0).get();
    return compareEntry<common::internalID_t>(idVector, vectorPos,
        entry + common::NullBuffer::getNumBytesForNullValues(
                    common::StructType::getNumFields(vector->dataType)));
}

static bool compareRelEntry(common::ValueVector* vector, uint32_t vectorPos, const uint8_t* entry) {
    KU_ASSERT(3 == common::StructType::getFieldIdx(vector->dataType, common::InternalKeyword::ID));
    auto idVector = common::StructVector::getFieldVector(vector, 3).get();
    return compareEntry<common::internalID_t>(idVector, vectorPos,
        entry + sizeof(common::internalID_t) * 2 + sizeof(common::ku_string_t) +
            common::NullBuffer::getNumBytesForNullValues(
                common::StructType::getNumFields(vector->dataType)));
}

template<>
[[maybe_unused]] bool compareEntry<struct_entry_t>(common::ValueVector* vector, uint32_t vectorPos,
    const uint8_t* entry) {
    switch (vector->dataType.getLogicalTypeID()) {
    case LogicalTypeID::NODE: {
        return compareNodeEntry(vector, vectorPos, entry);
    }
    case LogicalTypeID::REL: {
        return compareRelEntry(vector, vectorPos, entry);
    }
    case LogicalTypeID::STRUCT: {
        auto numFields = StructType::getNumFields(vector->dataType);
        auto entryToCompare = entry + NullBuffer::getNumBytesForNullValues(numFields);
        for (auto i = 0u; i < numFields; i++) {
            auto isNullInEntry = NullBuffer::isNull(entry, i);
            auto fieldVector = StructVector::getFieldVector(vector, i);
            compare_function_t compareFunc =
                getCompareEntryFunc(fieldVector->dataType.getPhysicalType());
            // Firstly check null on left and right side.
            if (isNullInEntry != fieldVector->isNull(vectorPos)) {
                return false;
            }
            // If both not null, compare the value.
            if (!isNullInEntry && !compareFunc(fieldVector.get(), vectorPos, entryToCompare)) {
                return false;
            }
            entryToCompare += LogicalTypeUtils::getRowLayoutSize(fieldVector->dataType);
        }
        return true;
    }
    default:
        KU_UNREACHABLE;
    }
}

static compare_function_t getCompareEntryFunc(PhysicalTypeID type) {
    compare_function_t func;
    TypeUtils::visit(
        type, [&]<HashableTypes T>(T) { func = compareEntry<T>; }, [](auto) { KU_UNREACHABLE; });
    return func;
}

void BaseHashTable::initSlotConstant(uint64_t numSlotsPerBlock_) {
    KU_ASSERT(numSlotsPerBlock_ == common::nextPowerOfTwo(numSlotsPerBlock_));
    numSlotsPerBlock = numSlotsPerBlock_;
    numSlotsPerBlockLog2 = std::log2(numSlotsPerBlock);
    slotIdxInBlockMask =
        common::BitmaskUtils::all1sMaskForLeastSignificantBits(numSlotsPerBlockLog2);
}

void BaseHashTable::initCompareFuncs() {
    compareEntryFuncs.reserve(keyTypes.size());
    for (auto i = 0u; i < keyTypes.size(); ++i) {
        compareEntryFuncs.push_back(getCompareEntryFunc(keyTypes[i].getPhysicalType()));
    }
}

void BaseHashTable::initTmpHashVector() {
    hashState = std::make_shared<DataChunkState>();
    hashState->setToFlat();
    hashVector = std::make_unique<ValueVector>(*LogicalType::HASH(), &memoryManager);
    hashVector->state = hashState;
}

} // namespace processor
} // namespace kuzu
