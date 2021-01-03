#include "src/processor/include/task_system/task.h"

namespace graphflow {
namespace processor {

void Task::run() {
    if (!registerThread()) {
        return;
    }
    QueryPlan planCopy{*plan};
    planCopy.initialize(&graph);
    planCopy.run();
    deregisterThread(planCopy.getPlanOutput());
}

bool Task::registerThread() {
    lock_t lck{mtx};
    if (canRegister()) {
        numThreadsRegistered++;
        return true;
    }
    return false;
}

void Task::deregisterThread(unique_ptr<PlanOutput> planOutput) {
    lock_t lck{mtx};
    threadOutputs.emplace_back(planOutput.release());
    if (numThreadsFinished == numThreadsRegistered - 1) {
        // last thread aggregates planOutputs from all threads.
        timer.stop();
        aggregatePlanOutputs();
    }
    ++numThreadsFinished;
}

void Task::aggregatePlanOutputs() {
    result = make_unique<pair<PlanOutput, chrono::milliseconds>>();
    PlanOutput::aggregate(threadOutputs, result->first);
    result->second = timer.getDuration();
}

} // namespace processor
} // namespace graphflow
