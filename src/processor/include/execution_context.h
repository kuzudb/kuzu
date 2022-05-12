#pragma once

#include "src/common/include/profiler.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/memory_manager.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::transaction;

namespace graphflow {
namespace processor {

struct ExecutionContext {

    ExecutionContext(uint64_t numThreads, Profiler* profiler, Transaction* transaction,
        MemoryManager* memoryManager, BufferManager* bufferManager)
        : numThreads{numThreads}, profiler{profiler}, transaction{transaction},
          memoryManager{memoryManager}, bufferManager{bufferManager} {}

    uint64_t numThreads;
    Profiler* profiler;
    Transaction* transaction;
    MemoryManager* memoryManager;
    BufferManager* bufferManager;
};

} // namespace processor
} // namespace graphflow
