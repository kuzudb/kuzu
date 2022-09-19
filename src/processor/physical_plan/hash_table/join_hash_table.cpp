#include "src/processor/include/physical_plan/hash_table/join_hash_table.h"

#include "src/function/hash/include/vector_hash_operations.h"

namespace graphflow {
namespace processor {

JoinHashTable::JoinHashTable(
    MemoryManager& memoryManager, uint64_t numTuples, unique_ptr<FactorizedTableSchema> tableSchema)
    : BaseHashTable{memoryManager} {
    maxNumHashSlots = HashTableUtils::nextPowerOfTwo(numTuples * 2);
    bitmask = maxNumHashSlots - 1;
    auto numHashSlotsPerBlock = LARGE_PAGE_SIZE / sizeof(uint8_t*);
    assert(numHashSlotsPerBlock == HashTableUtils::nextPowerOfTwo(numHashSlotsPerBlock));
    numSlotsPerBlockLog2 = log2(numHashSlotsPerBlock);
    slotIdxInBlockMask = BitmaskUtils::all1sMaskForLeastSignificantBits(numSlotsPerBlockLog2);
    auto numBlocks = (maxNumHashSlots + numHashSlotsPerBlock - 1) / numHashSlotsPerBlock;
    for (auto i = 0u; i < numBlocks; i++) {
        hashSlotsBlocks.emplace_back(make_unique<DataBlock>(&memoryManager));
    }
    // Prev pointer is always the last column in the table.
    colOffsetOfPrevPtrInTuple = tableSchema->getColOffset(tableSchema->getNumColumns() - 1);
    factorizedTable = make_unique<FactorizedTable>(&memoryManager, move(tableSchema));
}

// TODO(Guodong): refactor this function to partially re-use FactorizedTable::append, but calculate
// numTuplesToAppend here.
void JoinHashTable::append(const vector<shared_ptr<ValueVector>>& vectorsToAppend) {
    assert(factorizedTable->getTableSchema()->getColumn(0)->isFlat());
    auto numTuplesToAppend = vectorsToAppend[0]->state->isFlat() ?
                                 1 :
                                 vectorsToAppend[0]->state->selVector->selectedSize;
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

void JoinHashTable::probe(ValueVector& keyVector, uint8_t** probedTuples) {
    if (getNumTuples() == 0) {
        return;
    }
    auto hashVector = make_unique<ValueVector>(INT64, &memoryManager);
    auto hasNoNullValues = NodeIDVector::discardNull(keyVector);
    if (!hasNoNullValues) {
        return;
    }
    function::VectorHashOperations::computeHash(&keyVector, hashVector.get());
    auto hashes = (hash_t*)hashVector->values;
    auto startIdx = hashVector->state->isFlat() ? hashVector->state->currIdx : 0;
    auto numValues = hashVector->state->isFlat() ? 1 : hashVector->state->selVector->selectedSize;
    for (auto i = 0u; i < numValues; i++) {
        auto pos = hashVector->state->selVector->selectedPositions[i + startIdx];
        probedTuples[pos] = getTupleForHash(hashes[pos]);
    }
}

uint8_t** JoinHashTable::findHashSlot(const nodeID_t& value) const {
    hash_t hash;
    Hash::operation<nodeID_t>(value, false /* isNull */, hash);
    auto slotIdx = getSlotIdxForHash(hash);
    return (uint8_t**)(hashSlotsBlocks[slotIdx >> numSlotsPerBlockLog2]->getData() +
                       (slotIdx & slotIdxInBlockMask) * sizeof(uint8_t*));
}

uint8_t* JoinHashTable::insertEntry(uint8_t* tuple) const {
    auto slot = findHashSlot(*((nodeID_t*)tuple));
    auto prevPtr = *slot;
    *slot = tuple;
    return prevPtr;
}

} // namespace processor
} // namespace graphflow
