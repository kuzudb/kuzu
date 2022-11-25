#include "processor/operator/hash_join/join_hash_table.h"

#include "function/hash/vector_hash_operations.h"

namespace kuzu {
namespace processor {

JoinHashTable::JoinHashTable(MemoryManager& memoryManager, uint64_t numKeyColumns,
    unique_ptr<FactorizedTableSchema> tableSchema)
    : BaseHashTable{memoryManager}, numKeyColumns{numKeyColumns} {
    auto numSlotsPerBlock = LARGE_PAGE_SIZE / sizeof(uint8_t*);
    assert(numSlotsPerBlock == HashTableUtils::nextPowerOfTwo(numSlotsPerBlock));
    numSlotsPerBlockLog2 = log2(numSlotsPerBlock);
    slotIdxInBlockMask = BitmaskUtils::all1sMaskForLeastSignificantBits(numSlotsPerBlockLog2);
    // Prev pointer is always the last column in the table.
    colOffsetOfPrevPtrInTuple = tableSchema->getColOffset(tableSchema->getNumColumns() - 1);
    factorizedTable = make_unique<FactorizedTable>(&memoryManager, std::move(tableSchema));
}

bool JoinHashTable::discardNullFromKeys(
    const vector<shared_ptr<ValueVector>>& vectors, uint32_t numKeyVectors) {
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
// numTuplesToAppend here.
void JoinHashTable::append(const vector<shared_ptr<ValueVector>>& vectorsToAppend) {
    if (!discardNullFromKeys(vectorsToAppend, numKeyColumns)) {
        return;
    }
    // TODO(Guodong): use compiling information to remove the for loop.
    auto numTuplesToAppend = 1;
    for (auto i = 0u; i < numKeyColumns; i++) {
        numTuplesToAppend *= (vectorsToAppend[i]->state->isFlat() ?
                                  1 :
                                  vectorsToAppend[i]->state->selVector->selectedSize);
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
    maxNumHashSlots = HashTableUtils::nextPowerOfTwo(numTuples * 2);
    bitmask = maxNumHashSlots - 1;
    auto numSlotsPerBlock = (uint64_t)1 << numSlotsPerBlockLog2;
    auto numBlocksNeeded = (maxNumHashSlots + numSlotsPerBlock - 1) / numSlotsPerBlock;
    while (hashSlotsBlocks.size() < numBlocksNeeded) {
        hashSlotsBlocks.emplace_back(make_unique<DataBlock>(&memoryManager));
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

void JoinHashTable::probe(
    const vector<shared_ptr<ValueVector>>& keyVectors, uint8_t** probedTuples) {
    assert(keyVectors.size() == numKeyColumns);
    if (getNumTuples() == 0) {
        return;
    }
    if (!discardNullFromKeys(keyVectors, numKeyColumns)) {
        return;
    }
    auto hashVector = make_unique<ValueVector>(INT64, &memoryManager);
    unique_ptr<ValueVector> tmpHashVector =
        keyVectors.size() == 1 ? nullptr : make_unique<ValueVector>(INT64, &memoryManager);
    function::VectorHashOperations::computeHash(keyVectors[0].get(), hashVector.get());
    for (auto i = 1u; i < numKeyColumns; i++) {
        function::VectorHashOperations::computeHash(keyVectors[i].get(), tmpHashVector.get());
        function::VectorHashOperations::combineHash(
            hashVector.get(), tmpHashVector.get(), hashVector.get());
    }
    auto startIdx = hashVector->state->isFlat() ? hashVector->state->currIdx : 0;
    auto numValues = hashVector->state->isFlat() ? 1 : hashVector->state->selVector->selectedSize;
    for (auto i = 0u; i < numValues; i++) {
        auto pos = hashVector->state->selVector->selectedPositions[i + startIdx];
        probedTuples[i] = getTupleForHash(hashVector->getValue<hash_t>(pos));
    }
}

uint8_t** JoinHashTable::findHashSlot(nodeID_t* nodeIDs) const {
    hash_t hash;
    Hash::operation<nodeID_t>(nodeIDs[0], false /* isNull */, hash);
    for (auto i = 1u; i < numKeyColumns; i++) {
        hash_t newHash;
        Hash::operation<nodeID_t>(nodeIDs[i], false /* isNull */, newHash);
        CombineHash::operation(hash, newHash, hash);
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
