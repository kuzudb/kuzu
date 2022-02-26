#pragma once

#include "src/common/include/memory_manager.h"
#include "src/common/include/profiler.h"
#include "src/storage/include/buffer_manager.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

struct ExecutionContext {

public:
    ExecutionContext(Profiler& profiler, MemoryManager* memoryManager, BufferManager* bufferManager)
        : profiler{profiler}, memoryManager{memoryManager}, bufferManager{bufferManager} {}

public:
    Profiler& profiler;
    MemoryManager* memoryManager;
    BufferManager* bufferManager;
};

} // namespace processor
} // namespace graphflow
