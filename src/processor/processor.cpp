#include "src/processor/include/processor.h"

#include <iostream>

namespace graphflow {
namespace processor {

QueryProcessor::QueryProcessor(Graph& graph, const uint64_t& numThreads) : graph{graph} {
    for (auto n = 0u; n < numThreads; ++n) {
        threads.emplace_back([&] { run(); });
    }
}

QueryProcessor::~QueryProcessor() {
    stopThreads = true;
    for (auto& thread : threads) {
        thread.join();
    }
}

// This function is currently blocking. In the future, this should async and return the result
// wrapped in Future for syncing with the runner.
unique_ptr<pair<PlanOutput, chrono::milliseconds>> QueryProcessor::execute(
    unique_ptr<QueryPlan>& plan, const uint64_t& maxNumThreads) {
    auto task = make_shared<Task>(plan.get(), graph, maxNumThreads);
    queue.push(task);
    while (!task->isComplete()) {
        /*busy wait*/
    }
    return task->getResult();
}

void QueryProcessor::run() {
    while (true) {
        if (stopThreads) {
            break;
        }
        auto task = queue.getTask();
        if (!task) {
            /*busy wait*/
            continue;
        }
        task->run();
    }
}

} // namespace processor
} // namespace graphflow
