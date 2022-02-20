#pragma once

#include "src/common/include/memory_manager.h"
#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/utils.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"

using namespace graphflow::common;
using namespace operation;

namespace graphflow {
namespace processor {

class BaseHashTable {
public:
    BaseHashTable(MemoryManager& memoryManager) : memoryManager{memoryManager} {}

protected:
    uint64_t maxNumHashSlots;
    uint64_t bitMask;
    vector<unique_ptr<DataBlock>> hashSlotsBlocks;
    uint64_t numHashSlotsPerBlock;
    MemoryManager& memoryManager;
    unique_ptr<FactorizedTable> factorizedTable;
};

} // namespace processor
} // namespace graphflow
