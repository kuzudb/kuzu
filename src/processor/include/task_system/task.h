#pragma once

#include "src/common/include/timer.h"
#include "src/processor/include/plan/physical/physical_plan.h"
#include "src/processor/include/plan/physical/plan_output.h"

using namespace std;

namespace graphflow {
namespace processor {

using lock_t = unique_lock<mutex>;

class Task {

public:
    Task(PhysicalPlan* plan, Graph& graph, const uint64_t& maxNumThreads)
        : plan{plan}, graph{graph}, maxNumThreads{maxNumThreads}, timer{"task-timer"} {}

    void run();

    inline bool isComplete() {
        return numThreadsRegistered > 0 && numThreadsFinished == numThreadsRegistered;
    }

    inline bool canRegister() {
        return 0 == numThreadsFinished && maxNumThreads > numThreadsRegistered;
    }

    unique_ptr<pair<PlanOutput, chrono::milliseconds>> getResult() { return move(result); };

private:
    bool registerThread();
    void deregisterThread(unique_ptr<PlanOutput> planOutput);

    void aggregatePlanOutputs();

private:
    mutex mtx;
    PhysicalPlan* plan;
    Graph& graph;
    atomic_uint64_t maxNumThreads{0}, numThreadsFinished{0}, numThreadsRegistered{0};
    Timer timer;
    unique_ptr<pair<PlanOutput, chrono::milliseconds>> result;
    vector<unique_ptr<PlanOutput>> threadOutputs;
};

} // namespace processor
} // namespace graphflow
