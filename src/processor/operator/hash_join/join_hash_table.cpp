#include "processor/operator/hash_join/join_hash_table.h"

#include "common/utils.h"
#include "function/hash/vector_hash_functions.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

JoinHashTable::JoinHashTable(MemoryManager& memoryManager, logical_type_vec_t keyTypes,
    FactorizedTableSchema tableSchema)
    : BaseHashTable{memoryManager, std::move(keyTypes)} {
    auto numSlotsPerBlock = HASH_BLOCK_SIZE / sizeof(uint8_t*);
    initSlotConstant(numSlotsPerBlock);
    // Prev pointer is always the last column in the table.
    prevPtrColOffset = tableSchema.getColOffset(tableSchema.getNumColumns() - PREV_PTR_COL_IDX);
    factorizedTable = std::make_unique<FactorizedTable>(&memoryManager, std::move(tableSchema));
    this->tableSchema = factorizedTable->getTableSchema();
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
    auto numTuplesToAppend = keyState->getSelVector().getSelSize();
    auto appendInfos = factorizedTable->allocateFlatTupleBlocks(numTuplesToAppend);
    computeVectorHashes(keyVectors);
    auto colIdx = 0u;
    for (auto& vector : keyVectors) {
        appendVector(vector, appendInfos, colIdx++);
    }
    for (auto& vector : payloadVectors) {
        appendVector(vector, appendInfos, colIdx++);
    }
    appendVector(hashVector.get(), appendInfos, colIdx);
    factorizedTable->numTuples += numTuplesToAppend;
}

void JoinHashTable::appendVector(ValueVector* vector,
    const std::vector<BlockAppendingInfo>& appendInfos, ft_col_idx_t colIdx) {
    auto numAppendedTuples = 0ul;
    for (auto& blockAppendInfo : appendInfos) {
        factorizedTable->copyVectorToColumn(*vector, blockAppendInfo, numAppendedTuples, colIdx);
        numAppendedTuples += blockAppendInfo.numTuplesToAppend;
    }
}

static void sortSelectedPos(ValueVector* nodeIDVector) {
    auto& selVector = nodeIDVector->state->getSelVectorUnsafe();
    auto size = selVector.getSelSize();
    auto buffer = selVector.getMultableBuffer();
    if (selVector.isUnfiltered()) {
        std::memcpy(buffer.data(), &SelectionVector::INCREMENTAL_SELECTED_POS,
            size * sizeof(sel_t));
        selVector.setToFiltered();
    }
    std::sort(buffer.begin(), buffer.begin() + size, [nodeIDVector](sel_t left, sel_t right) {
        return nodeIDVector->getValue<nodeID_t>(left) < nodeIDVector->getValue<nodeID_t>(right);
    });
}

void JoinHashTable::appendVectorWithSorting(ValueVector* keyVector,
    std::vector<ValueVector*> payloadVectors) {
    auto numTuplesToAppend = 1;
    KU_ASSERT(keyVector->state->getSelVector().getSelSize() == 1);
    // Based on the way we are planning, we assume that the first and second vectors are both
    // nodeIDs from extending, while the first one is key, and the second one is payload.
    auto payloadNodeIDVector = payloadVectors[0];
    auto payloadsState = payloadNodeIDVector->state.get();
    if (!payloadsState->isFlat()) {
        // Sorting is only needed when the payload is unFlat (a list of values).
        sortSelectedPos(payloadNodeIDVector);
    }
    // A single appendInfo will return from `allocateFlatTupleBlocks` when numTuplesToAppend is 1.
    auto appendInfos = factorizedTable->allocateFlatTupleBlocks(numTuplesToAppend);
    KU_ASSERT(appendInfos.size() == 1);
    auto colIdx = 0u;
    std::vector<ValueVector*> keyVectors = {keyVector};
    computeVectorHashes(keyVectors);
    factorizedTable->copyVectorToColumn(*keyVector, appendInfos[0], numTuplesToAppend, colIdx++);
    for (auto& vector : payloadVectors) {
        factorizedTable->copyVectorToColumn(*vector, appendInfos[0], numTuplesToAppend, colIdx++);
    }
    factorizedTable->copyVectorToColumn(*hashVector, appendInfos[0], numTuplesToAppend, colIdx);
    if (!payloadsState->isFlat()) {
        // TODO(Xiyang): I can no longer recall why I set to un-filtered but this is probably wrong.
        // We should set back to the un-sorted state.
        payloadsState->getSelVectorUnsafe().setToUnfiltered();
    }
    factorizedTable->numTuples += numTuplesToAppend;
}

void JoinHashTable::allocateHashSlots(uint64_t numTuples) {
    setMaxNumHashSlots(nextPowerOfTwo(numTuples * 2));
    auto numSlotsPerBlock = (uint64_t)1 << numSlotsPerBlockLog2;
    auto numBlocksNeeded = (maxNumHashSlots + numSlotsPerBlock - 1) / numSlotsPerBlock;
    while (hashSlotsBlocks.size() < numBlocksNeeded) {
        hashSlotsBlocks.emplace_back(std::make_unique<DataBlock>(&memoryManager, HASH_BLOCK_SIZE));
    }
}

void JoinHashTable::buildHashSlots() {
    for (auto& tupleBlock : factorizedTable->getTupleDataBlocks()) {
        uint8_t* tuple = tupleBlock->getData();
        for (auto i = 0u; i < tupleBlock->numTuples; i++) {
            auto lastSlotEntryInHT = insertEntry(tuple);
            auto prevPtr = getPrevTuple(tuple);
            memcpy(reinterpret_cast<void*>(prevPtr), reinterpret_cast<void*>(&lastSlotEntryInHT),
                sizeof(uint8_t*));
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
    for (auto i = 0u; i < hashVector->state->getSelVector().getSelSize(); i++) {
        auto pos = hashVector->state->getSelVector()[i];
        KU_ASSERT(i < DEFAULT_VECTOR_CAPACITY);
        probedTuples[i] = getTupleForHash(hashVector->getValue<hash_t>(pos));
    }
}

sel_t JoinHashTable::matchFlatKeys(const std::vector<ValueVector*>& keyVectors,
    uint8_t** probedTuples, uint8_t** matchedTuples) {
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
    uint8_t** matchedTuples, SelectionVector& matchedTuplesSelVector) {
    auto numMatchedTuples = 0;
    for (auto i = 0u; i < keyVector->state->getSelVector().getSelSize(); ++i) {
        auto pos = keyVector->state->getSelVector()[i];
        while (probedTuples[i]) {
            auto currentTuple = probedTuples[i];
            auto entryCompareResult = compareEntryFuncs[0](keyVector, pos, currentTuple);
            if (entryCompareResult) {
                matchedTuples[numMatchedTuples] = currentTuple;
                matchedTuplesSelVector[numMatchedTuples] = pos;
                numMatchedTuples++;
                break;
            }
            probedTuples[i] = *getPrevTuple(currentTuple);
        }
    }
    return numMatchedTuples;
}

uint8_t** JoinHashTable::findHashSlot(const uint8_t* tuple) const {
    auto hash = *(hash_t*)(tuple + getHashValueColOffset());
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

bool JoinHashTable::compareFlatKeys(const std::vector<ValueVector*>& keyVectors,
    const uint8_t* tuple) {
    for (auto i = 0u; i < keyVectors.size(); i++) {
        auto keyVector = keyVectors[i];
        KU_ASSERT(keyVector->state->getSelVector().getSelSize() == 1);
        auto pos = keyVector->state->getSelVector()[0];
        auto equal = compareEntryFuncs[i](keyVector, pos, tuple + tableSchema->getColOffset(i));
        if (!equal) {
            return false;
        }
    }
    return true;
}

void JoinHashTable::computeVectorHashes(std::vector<common::ValueVector*> keyVectors) {
    std::vector<ValueVector*> dummyUnFlatKeyVectors;
    BaseHashTable::computeVectorHashes(keyVectors, dummyUnFlatKeyVectors);
}

offset_t JoinHashTable::getHashValueColOffset() const {
    return tableSchema->getColOffset(tableSchema->getNumColumns() - HASH_COL_IDX);
}

} // namespace processor
} // namespace kuzu
