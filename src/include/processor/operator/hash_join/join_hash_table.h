#pragma once

#include "common/utils.h"
#include "function/hash/hash_operations.h"
#include "processor/operator/base_hash_table.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::common;
using namespace kuzu::function::operation;

namespace kuzu {
namespace processor {

class JoinHashTable : public BaseHashTable {
public:
    JoinHashTable(MemoryManager& memoryManager, uint64_t numKeyColumns,
        unique_ptr<FactorizedTableSchema> tableSchema);

    virtual ~JoinHashTable() = default;

    virtual void append(const vector<shared_ptr<ValueVector>>& vectorsToAppend);
    void allocateHashSlots(uint64_t numTuples);
    void buildHashSlots();
    void probe(const vector<shared_ptr<ValueVector>>& keyVectors, uint8_t** probedTuples);

    inline void lookup(vector<shared_ptr<ValueVector>>& vectors, vector<uint32_t>& colIdxesToScan,
        uint8_t** tuplesToRead, uint64_t startPos, uint64_t numTuplesToRead) {
        factorizedTable->lookup(vectors, colIdxesToScan, tuplesToRead, startPos, numTuplesToRead);
    }
    inline void merge(JoinHashTable& other) { factorizedTable->merge(*other.factorizedTable); }
    inline uint64_t getNumTuples() { return factorizedTable->getNumTuples(); }
    inline uint8_t** getPrevTuple(const uint8_t* tuple) const {
        return (uint8_t**)(tuple + colOffsetOfPrevPtrInTuple);
    }
    inline uint8_t* getTupleForHash(hash_t hash) {
        auto slotIdx = getSlotIdxForHash(hash);
        return ((uint8_t**)(hashSlotsBlocks[slotIdx >> numSlotsPerBlockLog2]
                                ->getData()))[slotIdx & slotIdxInBlockMask];
    }
    inline FactorizedTable* getFactorizedTable() { return factorizedTable.get(); }
    inline const FactorizedTableSchema* getTableSchema() {
        return factorizedTable->getTableSchema();
    }

protected:
    uint8_t** findHashSlot(nodeID_t* nodeIDs) const;
    // This function returns the pointer that previously stored in the same slot.
    uint8_t* insertEntry(uint8_t* tuple) const;

    // This function returns a boolean flag indicating if there is non-null keys after discarding.
    static bool discardNullFromKeys(
        const vector<shared_ptr<ValueVector>>& vectors, uint32_t numKeyVectors);

private:
    uint64_t numKeyColumns;
    uint64_t colOffsetOfPrevPtrInTuple;
};

} // namespace processor
} // namespace kuzu
