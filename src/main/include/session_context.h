#pragma once

#include "src/common/include/profiler.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::common;
using namespace graphflow::transaction;

namespace graphflow {
namespace main {

struct SessionContext {

public:
    SessionContext() { profiler = make_unique<Profiler>(); }

public:
    string query;
    uint64_t numThreads = 1;
    unique_ptr<Profiler> profiler;
    Transaction* activeTransaction{nullptr};
};

} // namespace main
} // namespace graphflow
