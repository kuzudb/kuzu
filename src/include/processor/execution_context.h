#pragma once

#include "common/profiler.h"
#include "main/client_context.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace processor {

struct ExecutionContext {

    ExecutionContext(uint64_t numThreads, common::Profiler* profiler,
        storage::MemoryManager* memoryManager, storage::BufferManager* bufferManager,
        main::ClientContext* clientContext)
        : numThreads{numThreads}, profiler{profiler}, memoryManager{memoryManager},
          bufferManager{bufferManager}, transaction{nullptr}, clientContext{clientContext} {}

    uint64_t numThreads;
    common::Profiler* profiler;
    storage::MemoryManager* memoryManager;
    storage::BufferManager* bufferManager;

    transaction::Transaction* transaction;
    main::ClientContext* clientContext;
};

} // namespace processor
} // namespace kuzu
