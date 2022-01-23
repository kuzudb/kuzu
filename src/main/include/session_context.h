#pragma once

#include "src/common/include/profiler.h"
#include "src/main/include/plan_printer.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"

using namespace graphflow::common;

namespace graphflow {
namespace main {

struct SessionContext {

public:
    SessionContext() { profiler = make_unique<Profiler>(); }

    void clear() {
        profiler->resetMetrics();
        compilingTime = 0;
        executingTime = 0;
    };

public:
    // configurable user input
    string query;
    uint64_t numThreads = 1;
    bool enable_explain = false;

    unique_ptr<Profiler> profiler;
    unique_ptr<PlanPrinter> planPrinter;

    // query execution information that will always be returned to user regardless whether EXPLAIN
    // or PROFILE is enabled or not
    shared_ptr<FactorizedTable> queryResult;
    double compilingTime;
    double executingTime;
};

} // namespace main
} // namespace graphflow
