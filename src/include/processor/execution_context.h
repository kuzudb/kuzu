#pragma once

#include "common/profiler.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "transaction/transaction.h"

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
