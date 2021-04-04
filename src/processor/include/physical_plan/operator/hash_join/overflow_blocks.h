#pragma once

#include <cstdlib>

#include "src/common/include/types.h"
#include "src/common/include/vector/value_vector.h"
#include "src/processor/include/memory_manager.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace processor {

// For now, 2MB is a hard limit that the size of every value appended here should be less than this.
constexpr static const int64_t OVERFLOW_BLOCK_SIZE = 2 * 1024 * 1024;

class OverflowBlocks {
public:
    explicit OverflowBlocks(MemoryManager* memManager) : memManager(memManager) {}

    // Add a vector as an overflow value
    overflow_value_t addVector(ValueVector& vector);

    MemoryManager* memManager;

private:
    mutex overflowBlocksLock;
    vector<unique_ptr<BlockHandle>> blocks;
};
} // namespace processor
} // namespace graphflow
