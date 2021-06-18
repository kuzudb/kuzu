#pragma once

#include "src/common/include/profiler.h"
#include "src/main/include/plan_printer.h"
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
    bool enable_explain = false;
    uint64_t numThreads = 1;
    unique_ptr<Profiler> profiler;
    unique_ptr<PlanPrinter> planPrinter;
    Transaction* activeTransaction{nullptr};
};

} // namespace main
} // namespace graphflow
