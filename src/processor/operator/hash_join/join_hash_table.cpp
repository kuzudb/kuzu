#include "processor/operator/hash_join/join_hash_table.h"

#include "function/hash/vector_hash_functions.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

JoinHashTable::JoinHashTable(MemoryManager& memoryManager, uint64_t numKeyColumns,
    std::unique_ptr<FactorizedTableSchema> tableSchema)
    : BaseHashTable{memoryManager}, numKeyColumns{numKeyColumns} {
    auto numSlotsPerBlock = BufferPoolConstants::PAGE_256KB_SIZE / sizeof(uint8_t*);
    assert(numSlotsPerBlock == nextPowerOfTwo(numSlotsPerBlock));
    numSlotsPerBlockLog2 = std::log2(numSlotsPerBlock);
    slotIdxInBlockMask = BitmaskUtils::all1sMaskForLeastSignificantBits(numSlotsPerBlockLog2);
    // Prev pointer is always the last column in the table.
    colOffsetOfPrevPtrInTuple = tableSchema->getColOffset(tableSchema->getNumColumns() - 1);
    factorizedTable = std::make_unique<FactorizedTable>(&memoryManager, std::move(tableSchema));
}

bool JoinHashTable::discardNullFromKeys(
    const std::vector<ValueVector*>& vectors, uint32_t numKeyVectors) {
    bool hasNonNullKeys = true;
    for (auto i = 0u; i < numKeyVectors; i++) {
        if (!NodeIDVector::discardNull(*vectors[i])) {
            hasNonNullKeys = false;
            break;
        }
    }
    return hasNonNullKeys;
}

// TODO(Guodong): refactor this function to partially re-use FactorizedTable::append, but calculate
// numTuplesToAppend here. The refactor should also handle the case where multiple unFlat vectors
// are in the same dataChunk.
void JoinHashTable::append(const std::vector<ValueVector*>& vectorsToAppend) {
    if (!discardNullFromKeys(vectorsToAppend, numKeyColumns)) {
        return;
    }
    // TODO(Guodong): use compiling information to remove the for loop.
    auto numTuplesToAppend = 1;
    for (auto i = 0u; i < numKeyColumns; i++) {
        // At most one unFlat key data chunk. If there are multiple unFlat key vectors, they must
        // share the same state.
        if (!vectorsToAppend[i]->state->isFlat()) {
            numTuplesToAppend = vectorsToAppend[i]->state->selVector->selectedSize;
            break;
        }
    }
    auto appendInfos = factorizedTable->allocateFlatTupleBlocks(numTuplesToAppend);
    for (auto i = 0u; i < vectorsToAppend.size(); i++) {
        auto numAppendedTuples = 0ul;
        for (auto& blockAppendInfo : appendInfos) {
            factorizedTable->copyVectorToColumn(
                *vectorsToAppend[i], blockAppendInfo, numAppendedTuples, i);
            numAppendedTuples += blockAppendInfo.numTuplesToAppend;
        }
    }
    factorizedTable->numTuples += numTuplesToAppend;
}

void JoinHashTable::allocateHashSlots(uint64_t numTuples) {
    maxNumHashSlots = nextPowerOfTwo(numTuples * 2);
    bitmask = maxNumHashSlots - 1;
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

void JoinHashTable::probe(const std::vector<ValueVector*>& keyVectors,
    common::ValueVector* hashVector, common::ValueVector* tmpHashVector, uint8_t** probedTuples) {
    assert(keyVectors.size() == numKeyColumns);
    if (getNumTuples() == 0) {
        return;
    }
    if (!discardNullFromKeys(keyVectors, numKeyColumns)) {
        return;
    }
    function::VectorHashFunction::computeHash(keyVectors[0], hashVector);
    for (auto i = 1u; i < numKeyColumns; i++) {
        function::VectorHashFunction::computeHash(keyVectors[i], tmpHashVector);
        function::VectorHashFunction::combineHash(hashVector, tmpHashVector, hashVector);
    }
    for (auto i = 0u; i < hashVector->state->selVector->selectedSize; i++) {
        auto pos = hashVector->state->selVector->selectedPositions[i];
        probedTuples[i] = getTupleForHash(hashVector->getValue<hash_t>(pos));
    }
}

uint8_t** JoinHashTable::findHashSlot(nodeID_t* nodeIDs) const {
    hash_t hash;
    function::Hash::operation<nodeID_t>(nodeIDs[0], false /* isNull */, hash);
    for (auto i = 1u; i < numKeyColumns; i++) {
        hash_t newHash;
        function::Hash::operation<nodeID_t>(nodeIDs[i], false /* isNull */, newHash);
        function::CombineHash::operation(hash, newHash, hash);
    }
    auto slotIdx = getSlotIdxForHash(hash);
    return (uint8_t**)(hashSlotsBlocks[slotIdx >> numSlotsPerBlockLog2]->getData() +
                       (slotIdx & slotIdxInBlockMask) * sizeof(uint8_t*));
}

uint8_t* JoinHashTable::insertEntry(uint8_t* tuple) const {
    auto slot = findHashSlot((nodeID_t*)tuple);
    auto prevPtr = *slot;
    *slot = tuple;
    return prevPtr;
}

} // namespace processor
} // namespace kuzu
