#pragma once

#include "src/main/include/profiler.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::transaction;

namespace graphflow {
namespace main {

struct SessionContext {

public:
    SessionContext() : profiler{Profiler()} {};

public:
    string query;
    uint64_t numThreads = 1;
    Profiler profiler;
    Transaction* activeTransaction{nullptr};
};

} // namespace main
} // namespace graphflow
