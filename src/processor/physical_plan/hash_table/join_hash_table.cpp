#include "src/processor/include/physical_plan/hash_table/join_hash_table.h"

namespace graphflow {
namespace processor {

JoinHashTable::JoinHashTable(
    MemoryManager& memoryManager, uint64_t numTuples, unique_ptr<TableSchema> tableSchema)
    : BaseHashTable{memoryManager} {
    maxNumHashSlots = HashTableUtils::nextPowerOfTwo(numTuples * 2);
    bitMask = maxNumHashSlots - 1;
    auto numHashSlotsPerBlock = LARGE_PAGE_SIZE / sizeof(uint8_t*);
    assert(numHashSlotsPerBlock == HashTableUtils::nextPowerOfTwo(numHashSlotsPerBlock));
    numSlotsPerBlockLog2 = log2(numHashSlotsPerBlock);
    slotIdxInBlockMask = BitmaskUtils::all1sMaskForLeastSignificantBits(numSlotsPerBlockLog2);
    auto numBlocks = (maxNumHashSlots + numHashSlotsPerBlock - 1) / numHashSlotsPerBlock;
    for (auto i = 0u; i < numBlocks; i++) {
        hashSlotsBlocks.emplace_back(make_unique<DataBlock>(&memoryManager));
    }
    factorizedTable = make_unique<FactorizedTable>(&memoryManager, move(tableSchema));
}

template<typename T>
uint8_t** JoinHashTable::findHashEntry(T value) const {
    hash_t hashValue;
    Hash::operation<T>(value, false /* isNull */, hashValue);
    auto slotIdx = hashValue & bitMask;
    return (uint8_t**)(hashSlotsBlocks[slotIdx >> numSlotsPerBlockLog2]->getData() +
                       (slotIdx & slotIdxInBlockMask) * sizeof(uint8_t*));
}

template<typename T>
uint8_t* JoinHashTable::insertEntry(uint8_t* valueBuffer) {
    auto slotBuffer = findHashEntry(*((T*)valueBuffer));
    auto prevPtr = *slotBuffer;
    *slotBuffer = valueBuffer;
    return prevPtr;
}

void JoinHashTable::allocateHashSlots(uint64_t numTuples) {
    maxNumHashSlots = HashTableUtils::nextPowerOfTwo(numTuples * 2);
    bitMask = maxNumHashSlots - 1;
    auto numSlotsPerBlock = (uint64_t)1 << numSlotsPerBlockLog2;
    auto numBlocksNeeded = (maxNumHashSlots + numSlotsPerBlock - 1) / numSlotsPerBlock;
    while (hashSlotsBlocks.size() < numBlocksNeeded) {
        hashSlotsBlocks.emplace_back(make_unique<DataBlock>(&memoryManager));
    }
}

// TODO(Guodong): refactor this function to partially re-use FactorizedTable::append, but calculate
// numTuplesToAppend here.
void JoinHashTable::append(const vector<shared_ptr<ValueVector>>& vectorsToAppend) {
    assert(factorizedTable->getTableSchema()->getColumn(0)->isFlat());
    auto numTuplesToAppend =
        vectorsToAppend[0]->state->isFlat() ? 1 : vectorsToAppend[0]->state->selectedSize;
    auto appendInfos = factorizedTable->allocateTupleBlocks(numTuplesToAppend);
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

template uint8_t** JoinHashTable::findHashEntry<nodeID_t>(nodeID_t value) const;
template uint8_t* JoinHashTable::insertEntry<nodeID_t>(uint8_t* valueBuffer);

} // namespace processor
} // namespace graphflow
