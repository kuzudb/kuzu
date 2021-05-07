#include "src/processor/include/processor.h"

#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"
#include "src/processor/include/physical_plan/operator/sink/sink.h"
#include "src/processor/include/physical_plan/physical_plan.h"
#include "src/processor/include/physical_plan/query_result.h"

using namespace graphflow::common;
using namespace graphflow::planner;

namespace graphflow {
namespace processor {

QueryProcessor::QueryProcessor(uint64_t numThreads)
    : logger{spdlog::stdout_logger_st("processor")} {
    memManager = make_unique<MemoryManager>();
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
unique_ptr<QueryResult> QueryProcessor::execute(
    unique_ptr<LogicalPlan> plan, uint64_t maxNumThreads, const Graph& graph) {
    auto physicalPlan = PlanMapper::mapToPhysical(move(plan), graph);
    return execute(move(physicalPlan), maxNumThreads);
}

unique_ptr<QueryResult> QueryProcessor::execute(
    unique_ptr<PhysicalPlan> plan, uint64_t maxNumThreads) {
    auto resultCollector = reinterpret_cast<ResultCollector*>(plan->lastOperator.get());
    // The root pipeline(task) consists of operators and its prevOperator only, because by default,
    // our plan is a linear one. For binary operators, e.g., HashJoin, we always keep probe and its
    // prevOperator in the same pipeline, and decompose build and its prevOperator into another one.
    auto task = make_unique<Task>(resultCollector, maxNumThreads);
    decomposePlanIntoTasks(plan->lastOperator.get(), maxNumThreads, task.get());
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
    PhysicalOperator* op, uint64_t maxNumThreads, Task* parentTask) {
    switch (op->operatorType) {
    case HASH_JOIN_PROBE: {
        if (op->isOutDataChunkFiltered) {
            auto hashJoinProbe = reinterpret_cast<HashJoinProbe<true>*>(op);
            auto hashJoinBuild =
                reinterpret_cast<HashJoinBuild*>(hashJoinProbe->buildSidePrevOp.get());
            hashJoinBuild->setMemoryManager(memManager.get());
            decomposePlanIntoTasks(hashJoinBuild, maxNumThreads, parentTask);
        } else {
            auto hashJoinProbe = reinterpret_cast<HashJoinProbe<false>*>(op);
            auto hashJoinBuild =
                reinterpret_cast<HashJoinBuild*>(hashJoinProbe->buildSidePrevOp.get());
            hashJoinBuild->setMemoryManager(memManager.get());
            decomposePlanIntoTasks(hashJoinBuild, maxNumThreads, parentTask);
        }
        break;
    }
    case HASH_JOIN_BUILD: {
        auto hashJoinBuild = reinterpret_cast<HashJoinBuild*>(op);
        auto childTask = make_unique<Task>(reinterpret_cast<Sink*>(hashJoinBuild), maxNumThreads);
        decomposePlanIntoTasks(op->prevOperator.get(), maxNumThreads, childTask.get());
        childTask->parent = parentTask;
        parentTask->children.push_back(move(childTask));
        break;
    }
    case SCAN:
        break;
    default:
        decomposePlanIntoTasks(op->prevOperator.get(), maxNumThreads, parentTask);
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
            this_thread::sleep_for(chrono::milliseconds(10));
            continue;
        }
        task->run();
    }
}

} // namespace processor
} // namespace graphflow
