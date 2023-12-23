#pragma once

#include "common/profiler.h"
#include "main/client_context.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace processor {

struct ExecutionContext {
    uint64_t numThreads;
    common::Profiler* profiler;
    storage::MemoryManager* memoryManager;
    storage::BufferManager* bufferManager;
    main::ClientContext* clientContext;
    common::VirtualFileSystem* vfs;
    main::Database* database;

    ExecutionContext(uint64_t numThreads, common::Profiler* profiler,
        storage::MemoryManager* memoryManager, storage::BufferManager* bufferManager,
        main::ClientContext* clientContext, common::VirtualFileSystem* vfs,
        main::Database* database)
        : numThreads{numThreads}, profiler{profiler}, memoryManager{memoryManager},
          bufferManager{bufferManager}, clientContext{clientContext}, vfs{vfs}, database{database} {
    }
};

} // namespace processor
} // namespace kuzu
