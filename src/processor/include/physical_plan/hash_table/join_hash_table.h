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
    JoinHashTable(
        MemoryManager& memoryManager, uint64_t numTuples, unique_ptr<TableSchema> tableSchema);

    template<typename T>
    uint8_t** findHashEntry(T value) const;
    // This function returns the pointer that previously stored in the same slot.
    template<typename T>
    uint8_t* insertEntry(uint8_t* valueBuffer);

    void allocateHashSlots(uint64_t numTuples);
    void append(const vector<shared_ptr<ValueVector>>& vectorsToAppend);
    inline FactorizedTable* getFactorizedTable() { return factorizedTable.get(); }

private:
    // NOTE: In vectorsToAppend, we need to guarantee that the first vector is the join key.
    void appendForFlatKeys(const vector<shared_ptr<ValueVector>>& vectorsToAppend);
    void appendForUnflatKeys(const vector<shared_ptr<ValueVector>>& vectorsToAppend);
};

} // namespace processor
} // namespace graphflow
