#pragma once

#include "src/common/include/profiler.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/memory_manager.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::transaction;

namespace graphflow {
namespace processor {

struct ExecutionContext {

    ExecutionContext(uint64_t numThreads, Profiler* profiler, MemoryManager* memoryManager,
        BufferManager* bufferManager)
        : numThreads{numThreads}, profiler{profiler}, memoryManager{memoryManager},
          bufferManager{bufferManager} {}

    uint64_t numThreads;
    Profiler* profiler;
    MemoryManager* memoryManager;
    BufferManager* bufferManager;

    Transaction* transaction;
};

} // namespace processor
} // namespace graphflow
