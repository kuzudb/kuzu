#pragma once

#include <atomic>
#include <memory>
#include <utility>

#include "src/processor/include/task_system/task_queue.h"
#include "src/storage/include/graph.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class QueryProcessor {

public:
    QueryProcessor(const uint64_t& numThreads);
    ~QueryProcessor();

    unique_ptr<pair<PlanOutput, chrono::milliseconds>> execute(
        unique_ptr<PhysicalPlan>& plan, Graph& graph, const uint64_t& maxNumThreads);

private:
    void run();

private:
    shared_ptr<spdlog::logger> logger;
    TaskQueue queue;
    condition_variable ready;
    bool stopThreads{false};
    vector<thread> threads;
};

} // namespace processor
} // namespace graphflow
