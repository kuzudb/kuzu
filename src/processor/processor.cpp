#include "src/processor/include/processor.h"

#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/read_list/frontier_extend.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"
#include "src/processor/include/physical_plan/operator/sink/sink.h"
#include "src/processor/include/physical_plan/query_result.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

QueryProcessor::QueryProcessor(uint64_t numThreads) {
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

unique_ptr<QueryResult> QueryProcessor::execute(PhysicalPlan* physicalPlan, uint64_t numThreads) {
    auto lastOperator = physicalPlan->lastOperator.get();
    auto resultCollector = reinterpret_cast<ResultCollector*>(lastOperator);
    // The root pipeline(task) consists of operators and its prevOperator only, because by default,
    // our plan is a linear one. For binary operators, e.g., HashJoin, we always keep probe and its
    // prevOperator in the same pipeline, and decompose build and its prevOperator into another one.
    auto task = make_unique<Task>(resultCollector, numThreads);
    decomposePlanIntoTasks(lastOperator, task.get(), numThreads);
    scheduleTask(task.get());
    while (!task->isCompleted()) {
        /*busy wait*/
        this_thread::sleep_for(chrono::milliseconds(10));
    }
    return move(resultCollector->queryResult);
}

void QueryProcessor::scheduleTask(Task* task) {
    if (task->children.empty()) {
        queue.push(task);
    } else {
        for (auto& dependency : task->children) {
            scheduleTask(dependency.get());
        }
        while (task->numDependenciesFinished.load() < task->children.size()) {}
        queue.push(task);
    }
}

void QueryProcessor::decomposePlanIntoTasks(
    PhysicalOperator* op, Task* parentTask, uint64_t numThreads) {
    switch (op->operatorType) {
    case HASH_JOIN_PROBE: {
        if (op->isOutDataChunkFiltered) {
            auto hashJoinProbe = reinterpret_cast<HashJoinProbe<true>*>(op);
            auto hashJoinBuild =
                reinterpret_cast<HashJoinBuild*>(hashJoinProbe->buildSidePrevOp.get());
            decomposePlanIntoTasks(hashJoinBuild, parentTask, numThreads);
        } else {
            auto hashJoinProbe = reinterpret_cast<HashJoinProbe<false>*>(op);
            auto hashJoinBuild =
                reinterpret_cast<HashJoinBuild*>(hashJoinProbe->buildSidePrevOp.get());
            decomposePlanIntoTasks(hashJoinBuild, parentTask, numThreads);
        }
        decomposePlanIntoTasks(op->prevOperator.get(), parentTask, numThreads);
        break;
    }
    case HASH_JOIN_BUILD: {
        auto hashJoinBuild = reinterpret_cast<HashJoinBuild*>(op);
        auto childTask = make_unique<Task>(reinterpret_cast<Sink*>(hashJoinBuild), numThreads);
        decomposePlanIntoTasks(op->prevOperator.get(), childTask.get(), numThreads);
        childTask->parent = parentTask;
        parentTask->children.push_back(move(childTask));
        break;
    }
    case SCAN:
    case LOAD_CSV:
        break;
    default:
        decomposePlanIntoTasks(op->prevOperator.get(), parentTask, numThreads);
        break;
    }
}

void QueryProcessor::run() {
    while (true) {
        if (stopThreads) {
            break;
        }
        auto task = queue.getTask();
        if (!task) {
            this_thread::sleep_for(chrono::microseconds(100));
            continue;
        }
        task->run();
    }
}

} // namespace processor
} // namespace graphflow
