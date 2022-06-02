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
    JoinHashTable(MemoryManager& memoryManager, uint64_t numTuples, TableSchema& tableSchema);

    template<class T>
    uint8_t** findHashEntry(T value) const {
        hash_t hashValue;
        Hash::operation<T>(value, false /* isNull */, hashValue);
        auto slotIdx = hashValue & bitMask;
        return (uint8_t**)(hashSlotsBlocks[slotIdx / numHashSlotsPerBlock]->getData() +
                           slotIdx % numHashSlotsPerBlock * sizeof(uint8_t*));
    }

    // This function returns the pointer that previously stored in the same slot.
    template<class T>
    uint8_t* insertEntry(uint8_t* valueBuffer) {
        auto slotBuffer = findHashEntry(*((T*)valueBuffer));
        auto prevPtr = *slotBuffer;
        *slotBuffer = valueBuffer;
        return prevPtr;
    }

    void allocateHashSlots(uint64_t numTuples);

    FactorizedTable* getFactorizedTable() { return factorizedTable.get(); }
};

} // namespace processor
} // namespace graphflow
