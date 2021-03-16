#pragma once

#include <cstdlib>

#include "src/common/include/types.h"
#include "src/processor/include/memory_manager.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace processor {

// For now, 2MB is a hard limit that the size of every value appended here should be less than this.
constexpr static const int64_t OVERFLOW_BLOCK_SIZE = 2 * 1024 * 1024;

class OverflowBlocks {
public:
    explicit OverflowBlocks(MemoryManager& memoryManager) : memoryManager(memoryManager) {}

    overflow_value_t addValue(uint8_t* valuePtr, uint64_t valueLength);

private:
    MemoryManager& memoryManager;
    vector<unique_ptr<BlockHandle>> blocks;
};
} // namespace processor
} // namespace graphflow
