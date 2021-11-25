#pragma once

#include "src/common/include/memory_manager.h"
#include "src/common/include/profiler.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct ExecutionContext {

public:
    ExecutionContext(Profiler& profiler, MemoryManager* memoryManager)
        : profiler{profiler}, memoryManager{memoryManager} {}

public:
    Profiler& profiler;
    MemoryManager* memoryManager;
};

} // namespace processor
} // namespace graphflow
