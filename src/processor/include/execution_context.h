#pragma once

#include "src/common/include/profiler.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::transaction;

namespace graphflow {
namespace processor {

struct ExecutionContext {

public:
    ExecutionContext(Profiler& profiler, Transaction* transaction, MemoryManager* memoryManager)
        : profiler{profiler}, transaction{transaction}, memoryManager{memoryManager} {}

public:
    Profiler& profiler;
    Transaction* transaction;
    MemoryManager* memoryManager;
};

} // namespace processor
} // namespace graphflow
