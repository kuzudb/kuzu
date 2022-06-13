#pragma once

#include "src/common/include/utils.h"
#include "src/function/hash/operations/include/hash_operations.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"
#include "src/storage/buffer_manager/include/memory_manager.h"

using namespace graphflow::common;
using namespace graphflow::function::operation;

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
