#pragma once

#include "src/common/include/utils.h"
#include "src/function/hash/operations/include/hash_operations.h"
#include "src/processor/include/physical_plan/hash_table/base_hash_table.h"
#include "src/storage/buffer_manager/include/memory_manager.h"

using namespace graphflow::common;
using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

class JoinHashTable : public BaseHashTable {
public:
    JoinHashTable(MemoryManager& memoryManager, uint64_t numTuples,
        unique_ptr<FactorizedTableSchema> tableSchema);

    void append(const vector<shared_ptr<ValueVector>>& vectorsToAppend);
    void allocateHashSlots(uint64_t numTuples);
    void buildHashSlots();
    void probe(ValueVector& keyVector, uint8_t** probedTuples);

    inline void lookup(vector<shared_ptr<ValueVector>>& vectors, vector<uint32_t>& colIdxesToScan,
        uint8_t** tuplesToRead, uint64_t startPos, uint64_t numTuplesToRead) {
        factorizedTable->lookup(vectors, colIdxesToScan, tuplesToRead, startPos, numTuplesToRead);
    }
    inline void merge(JoinHashTable& other) { factorizedTable->merge(*other.factorizedTable); }
    inline uint64_t getNumTuples() { return factorizedTable->getNumTuples(); }
    inline uint8_t** getPrevTuple(const uint8_t* tuple) const {
        return (uint8_t**)(tuple + colOffsetOfPrevPtrInTuple);
    }
    inline bool hasUnflatColumns() { return factorizedTable->hasUnflatCol(); }

private:
    uint8_t** findHashSlot(const nodeID_t& value) const;
    // This function returns the pointer that previously stored in the same slot.
    uint8_t* insertEntry(uint8_t* tuple) const;

    inline uint8_t* getTupleForHash(hash_t hash) {
        auto slotIdx = getSlotIdxForHash(hash);
        return ((uint8_t**)(hashSlotsBlocks[slotIdx >> numSlotsPerBlockLog2]
                                ->getData()))[slotIdx & slotIdxInBlockMask];
    }

private:
    uint64_t colOffsetOfPrevPtrInTuple;
};

} // namespace processor
} // namespace graphflow
