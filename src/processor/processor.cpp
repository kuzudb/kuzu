#include "src/processor/include/processor.h"

#include <iostream>

namespace graphflow {
namespace processor {

QueryProcessor::QueryProcessor(const uint64_t& numThreads)
    : logger{spdlog::stdout_logger_st("processor")} {
    for (auto n = 0u; n < numThreads; ++n) {
        threads.emplace_back([&] { run(); });
    }
    logger->info("Processor started with {} threads.", numThreads);
}

QueryProcessor::~QueryProcessor() {
    stopThreads = true;
    for (auto& thread : threads) {
        thread.join();
    }
    spdlog::drop("processor");
}

// This function is currently blocking. In the future, this should async and return the result
// wrapped in Future for syncing with the runner.
unique_ptr<pair<PlanOutput, chrono::milliseconds>> QueryProcessor::execute(
    unique_ptr<PhysicalPlan>& plan, Graph& graph, const uint64_t& maxNumThreads) {
    auto task = make_shared<Task>(plan.get(), graph, maxNumThreads);
    queue.push(task);
    while (!task->isComplete()) {
        /*busy wait*/
        this_thread::sleep_for(chrono::milliseconds(10));
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
            this_thread::sleep_for(chrono::milliseconds(10));
            continue;
        }
        task->run();
    }
}

} // namespace processor
} // namespace graphflow
