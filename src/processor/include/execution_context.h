#pragma once

#include "src/common/include/profiler.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/memory_manager.h"
#include "src/transaction/include/transaction.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

struct ExecutionContext {

    ExecutionContext(uint64_t numThreads, Profiler* profiler, MemoryManager* memoryManager,
        BufferManager* bufferManager)
        : numThreads{numThreads}, profiler{profiler}, memoryManager{memoryManager},
          bufferManager{bufferManager}, transaction{nullptr} {}

    uint64_t numThreads;
    Profiler* profiler;
    MemoryManager* memoryManager;
    BufferManager* bufferManager;

    Transaction* transaction;
};

} // namespace processor
} // namespace kuzu
