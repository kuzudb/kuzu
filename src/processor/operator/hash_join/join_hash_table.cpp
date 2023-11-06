#include "processor/operator/hash_join/join_hash_table.h"

#include "function/comparison/comparison_functions.h"
#include "function/hash/vector_hash_functions.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

JoinHashTable::JoinHashTable(MemoryManager& memoryManager,
    std::vector<std::unique_ptr<LogicalType>> keyTypes,
    std::unique_ptr<FactorizedTableSchema> tableSchema)
    : BaseHashTable{memoryManager}, keyTypes{std::move(keyTypes)} {
    auto numSlotsPerBlock = BufferPoolConstants::PAGE_256KB_SIZE / sizeof(uint8_t*);
    initSlotConstant(numSlotsPerBlock);
    // Prev pointer is always the last column in the table.
    prevPtrColOffset = tableSchema->getColOffset(tableSchema->getNumColumns() - 1);
    factorizedTable = std::make_unique<FactorizedTable>(&memoryManager, std::move(tableSchema));
    this->tableSchema = factorizedTable->getTableSchema();
    initFunctions();
}

void JoinHashTable::initFunctions() {
    entryHashFunctions.resize(keyTypes.size());
    entryCompareFunctions.resize(keyTypes.size());
    for (auto i = 0u; i < keyTypes.size(); ++i) {
        getHashFunction(keyTypes[i]->getPhysicalType(), entryHashFunctions[i]);
        getCompareFunction(keyTypes[i]->getPhysicalType(), entryCompareFunctions[i]);
    }
}

static bool discardNullFromKeys(const std::vector<ValueVector*>& vectors) {
    bool hasNonNullKeys = true;
    for (auto& vector : vectors) {
        if (!ValueVector::discardNull(*vector)) {
            hasNonNullKeys = false;
            break;
        }
    }
    return hasNonNullKeys;
}

void JoinHashTable::appendVectors(const std::vector<ValueVector*>& keyVectors,
    const std::vector<ValueVector*>& payloadVectors, DataChunkState* keyState) {
    discardNullFromKeys(keyVectors);
    auto numTuplesToAppend = keyState->selVector->selectedSize;
    auto appendInfos = factorizedTable->allocateFlatTupleBlocks(numTuplesToAppend);
    auto colIdx = 0u;
    for (auto& vector : keyVectors) {
        appendVector(vector, appendInfos, colIdx++);
    }
    for (auto& vector : payloadVectors) {
        appendVector(vector, appendInfos, colIdx++);
    }
    factorizedTable->numTuples += numTuplesToAppend;
}

void JoinHashTable::appendVector(
    ValueVector* vector, const std::vector<BlockAppendingInfo>& appendInfos, ft_col_idx_t colIdx) {
    auto numAppendedTuples = 0ul;
    for (auto& blockAppendInfo : appendInfos) {
        factorizedTable->copyVectorToColumn(*vector, blockAppendInfo, numAppendedTuples, colIdx);
        numAppendedTuples += blockAppendInfo.numTuplesToAppend;
    }
}

static void sortSelectedPos(ValueVector* nodeIDVector) {
    auto selVector = nodeIDVector->state->selVector.get();
    auto size = selVector->selectedSize;
    auto selectedPos = selVector->getSelectedPositionsBuffer();
    if (selVector->isUnfiltered()) {
        memcpy(selectedPos, &SelectionVector::INCREMENTAL_SELECTED_POS, size * sizeof(sel_t));
        selVector->resetSelectorToValuePosBuffer();
    }
    std::sort(selectedPos, selectedPos + size, [nodeIDVector](sel_t left, sel_t right) {
        return nodeIDVector->getValue<nodeID_t>(left) < nodeIDVector->getValue<nodeID_t>(right);
    });
}

void JoinHashTable::appendVectorWithSorting(
    ValueVector* keyVector, std::vector<ValueVector*> payloadVectors) {
    auto numTuplesToAppend = 1;
    KU_ASSERT(keyVector->state->selVector->selectedSize == 1);
    // Based on the way we are planning, we assume that the first and second vectors are both
    // nodeIDs from extending, while the first one is key, and the second one is payload.
    auto payloadNodeIDVector = payloadVectors[0];
    auto payloadsState = payloadNodeIDVector->state.get();
    if (!payloadsState->isFlat()) {
        // Sorting is only needed when the payload is unflat (a list of values).
        sortSelectedPos(payloadNodeIDVector);
    }
    // A single appendInfo will return from `allocateFlatTupleBlocks` when numTuplesToAppend is 1.
    auto appendInfos = factorizedTable->allocateFlatTupleBlocks(numTuplesToAppend);
    KU_ASSERT(appendInfos.size() == 1);
    auto colIdx = 0u;
    factorizedTable->copyVectorToColumn(*keyVector, appendInfos[0], numTuplesToAppend, colIdx++);
    for (auto& vector : payloadVectors) {
        factorizedTable->copyVectorToColumn(*vector, appendInfos[0], numTuplesToAppend, colIdx++);
    }
    if (!payloadsState->isFlat()) {
        payloadsState->selVector->resetSelectorToUnselected();
    }
    factorizedTable->numTuples += numTuplesToAppend;
}

void JoinHashTable::allocateHashSlots(uint64_t numTuples) {
    setMaxNumHashSlots(nextPowerOfTwo(numTuples * 2));
    auto numSlotsPerBlock = (uint64_t)1 << numSlotsPerBlockLog2;
    auto numBlocksNeeded = (maxNumHashSlots + numSlotsPerBlock - 1) / numSlotsPerBlock;
    while (hashSlotsBlocks.size() < numBlocksNeeded) {
        hashSlotsBlocks.emplace_back(std::make_unique<DataBlock>(&memoryManager));
    }
}

void JoinHashTable::buildHashSlots() {
    for (auto& tupleBlock : factorizedTable->getTupleDataBlocks()) {
        uint8_t* tuple = tupleBlock->getData();
        for (auto i = 0u; i < tupleBlock->numTuples; i++) {
            auto lastSlotEntryInHT = insertEntry(tuple);
            auto prevPtr = getPrevTuple(tuple);
            memcpy(prevPtr, &lastSlotEntryInHT, sizeof(uint8_t*));
            tuple += factorizedTable->getTableSchema()->getNumBytesPerTuple();
        }
    }
}

void JoinHashTable::probe(const std::vector<ValueVector*>& keyVectors, ValueVector* hashVector,
    ValueVector* tmpHashVector, uint8_t** probedTuples) {
    KU_ASSERT(keyVectors.size() == keyTypes.size());
    if (getNumTuples() == 0) {
        return;
    }
    if (!discardNullFromKeys(keyVectors)) {
        return;
    }
    function::VectorHashFunction::computeHash(keyVectors[0], hashVector);
    for (auto i = 1u; i < keyVectors.size(); i++) {
        function::VectorHashFunction::computeHash(keyVectors[i], tmpHashVector);
        function::VectorHashFunction::combineHash(hashVector, tmpHashVector, hashVector);
    }
    for (auto i = 0u; i < hashVector->state->selVector->selectedSize; i++) {
        auto pos = hashVector->state->selVector->selectedPositions[i];
        KU_ASSERT(i < DEFAULT_VECTOR_CAPACITY);
        probedTuples[i] = getTupleForHash(hashVector->getValue<hash_t>(pos));
    }
}

sel_t JoinHashTable::matchFlatKeys(
    const std::vector<ValueVector*>& keyVectors, uint8_t** probedTuples, uint8_t** matchedTuples) {
    auto numMatchedTuples = 0;
    while (probedTuples[0]) {
        if (numMatchedTuples == DEFAULT_VECTOR_CAPACITY) {
            break;
        }
        auto currentTuple = probedTuples[0];
        matchedTuples[numMatchedTuples] = currentTuple;
        numMatchedTuples += compareFlatKeys(keyVectors, currentTuple);
        probedTuples[0] = *getPrevTuple(currentTuple);
    }
    return numMatchedTuples;
}

sel_t JoinHashTable::matchUnFlatKey(ValueVector* keyVector, uint8_t** probedTuples,
    uint8_t** matchedTuples, SelectionVector* matchedTuplesSelVector) {
    auto numMatchedTuples = 0;
    for (auto i = 0u; i < keyVector->state->selVector->selectedSize; ++i) {
        auto pos = keyVector->state->selVector->selectedPositions[i];
        while (probedTuples[i]) {
            auto currentTuple = probedTuples[i];
            uint8_t entryCompareResult = false;
            entryCompareFunctions[0](*keyVector, pos, currentTuple, entryCompareResult);
            if (entryCompareResult) {
                matchedTuples[numMatchedTuples] = currentTuple;
                matchedTuplesSelVector->selectedPositions[numMatchedTuples] = pos;
                numMatchedTuples++;
                break;
            }
            probedTuples[i] = *getPrevTuple(currentTuple);
        }
    }
    return numMatchedTuples;
}

uint8_t** JoinHashTable::findHashSlot(uint8_t* tuple) const {
    auto idx = 0u;
    hash_t hash;
    entryHashFunctions[idx++](tuple, hash);
    hash_t tmpHash;
    while (idx < keyTypes.size()) {
        entryHashFunctions[idx](tuple + tableSchema->getColOffset(idx), tmpHash);
        function::CombineHash::operation(hash, tmpHash, hash);
        idx++;
    }
    auto slotIdx = getSlotIdxForHash(hash);
    return (uint8_t**)(hashSlotsBlocks[slotIdx >> numSlotsPerBlockLog2]->getData() +
                       (slotIdx & slotIdxInBlockMask) * sizeof(uint8_t*));
}

uint8_t* JoinHashTable::insertEntry(uint8_t* tuple) const {
    auto slot = findHashSlot(tuple);
    auto prevPtr = *slot;
    *slot = tuple;
    return prevPtr;
}

bool JoinHashTable::compareFlatKeys(
    const std::vector<ValueVector*>& keyVectors, const uint8_t* tuple) {
    uint8_t equal = false;
    for (auto i = 0u; i < keyVectors.size(); i++) {
        auto keyVector = keyVectors[i];
        KU_ASSERT(keyVector->state->selVector->selectedSize == 1);
        auto pos = keyVector->state->selVector->selectedPositions[0];
        entryCompareFunctions[i](*keyVector, pos, tuple + tableSchema->getColOffset(i), equal);
        if (!equal) {
            return false;
        }
    }
    return true;
}

template<typename T>
static void hashEntry(const uint8_t* entry, hash_t& result) {
    Hash::operation(*(T*)entry, result);
}

void JoinHashTable::getHashFunction(PhysicalTypeID physicalTypeID, hash_function_t& func) {
    switch (physicalTypeID) {
    case PhysicalTypeID::INTERNAL_ID: {
        func = hashEntry<nodeID_t>;
    } break;
    case PhysicalTypeID::BOOL: {
        func = hashEntry<bool>;
    } break;
    case PhysicalTypeID::INT64: {
        func = hashEntry<int64_t>;
    } break;
    case PhysicalTypeID::INT32: {
        func = hashEntry<int32_t>;
    } break;
    case PhysicalTypeID::INT16: {
        func = hashEntry<int16_t>;
    } break;
    case PhysicalTypeID::INT8: {
        func = hashEntry<int8_t>;
    }
    case PhysicalTypeID::UINT64: {
        func = hashEntry<uint64_t>;
    }
    case PhysicalTypeID::UINT32: {
        func = hashEntry<uint32_t>;
    }
    case PhysicalTypeID::UINT16: {
        func = hashEntry<uint16_t>;
    }
    case PhysicalTypeID::UINT8: {
        func = hashEntry<uint8_t>;
    }
    case PhysicalTypeID::INT128: {
        func = hashEntry<int128_t>;
    }
    case PhysicalTypeID::DOUBLE: {
        func = hashEntry<double_t>;
    } break;
    case PhysicalTypeID::FLOAT: {
        func = hashEntry<float_t>;
    } break;
    case PhysicalTypeID::STRING: {
        func = hashEntry<ku_string_t>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        func = hashEntry<interval_t>;
    } break;
    default: {
        throw RuntimeException("Join hash table cannot hash data type " +
                               PhysicalTypeUtils::physicalTypeToString(physicalTypeID));
    }
    }
}

template<typename T>
static void compareEntry(
    const ValueVector& vector, uint32_t pos, const uint8_t* entry, uint8_t& result) {
    Equals::operation(vector.getValue<T>(pos), *(T*)entry, result, nullptr /* leftVector */,
        nullptr /* rightVector */);
}

void JoinHashTable::getCompareFunction(
    PhysicalTypeID physicalTypeID, JoinHashTable::compare_function_t& func) {
    switch (physicalTypeID) {
    case PhysicalTypeID::INTERNAL_ID: {
        func = compareEntry<nodeID_t>;
    } break;
    case PhysicalTypeID::BOOL: {
        func = compareEntry<bool>;
    } break;
    case PhysicalTypeID::INT64: {
        func = compareEntry<int64_t>;
    } break;
    case PhysicalTypeID::INT32: {
        func = compareEntry<int32_t>;
    } break;
    case PhysicalTypeID::INT16: {
        func = compareEntry<int16_t>;
    } break;
    case PhysicalTypeID::INT8: {
        func = compareEntry<int8_t>;
    }
    case PhysicalTypeID::UINT64: {
        func = compareEntry<uint64_t>;
    }
    case PhysicalTypeID::UINT32: {
        func = compareEntry<uint32_t>;
    }
    case PhysicalTypeID::UINT16: {
        func = compareEntry<uint16_t>;
    }
    case PhysicalTypeID::UINT8: {
        func = compareEntry<uint8_t>;
    }
    case PhysicalTypeID::INT128: {
        func = compareEntry<int128_t>;
    }
    case PhysicalTypeID::DOUBLE: {
        func = compareEntry<double_t>;
    } break;
    case PhysicalTypeID::FLOAT: {
        func = compareEntry<float_t>;
    } break;
    case PhysicalTypeID::STRING: {
        func = compareEntry<ku_string_t>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        func = compareEntry<interval_t>;
    } break;
    default: {
        throw RuntimeException("Join hash table cannot compare data type " +
                               PhysicalTypeUtils::physicalTypeToString(physicalTypeID));
    }
    }
}

} // namespace processor
} // namespace kuzu
