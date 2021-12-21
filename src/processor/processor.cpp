#include "src/processor/include/processor.h"

#include <iostream>

#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/physical_plan/result/query_result.h"
#include "src/processor/include/processor_task.h"
using namespace graphflow::common;

namespace graphflow {
namespace processor {

QueryProcessor::QueryProcessor(uint64_t numThreads) {
    taskScheduler = make_unique<TaskScheduler>(numThreads);
}

unique_ptr<QueryResult> QueryProcessor::execute(PhysicalPlan* physicalPlan, uint64_t numThreads) {
    auto lastOperator = physicalPlan->lastOperator.get();
    auto resultCollector = reinterpret_cast<ResultCollector*>(lastOperator);
    // The root pipeline(task) consists of operators and its prevOperator only, because we
    // expect to have linear plans. For binary operators, e.g., HashJoin, we  keep probe and its
    // prevOperator in the same pipeline, and decompose build and its prevOperator into another one.
    auto task = make_shared<ProcessorTask>(resultCollector, numThreads);
    decomposePlanIntoTasks(lastOperator, task.get(), numThreads);
    cout << "task.children.size(): " << to_string(task->children.size()) << endl;
    taskScheduler->scheduleTaskAndWaitOrError(task);
    cout << "Finished executing task" << endl;
    cout << "resultCollector->queryResult->numTuples: "
         << to_string(resultCollector->queryResult->numTuples) << endl;
    // this_thread::sleep_for(chrono::microseconds(3000000));
    return move(resultCollector->queryResult);
}

void QueryProcessor::decomposePlanIntoTasks(
    PhysicalOperator* op, Task* parentTask, uint64_t numThreads) {
    switch (op->operatorType) {
    case HASH_JOIN_PROBE: {
        auto hashJoinProbe = reinterpret_cast<HashJoinProbe*>(op);
        decomposePlanIntoTasks(hashJoinProbe->buildSidePrevOp.get(), parentTask, numThreads);
        decomposePlanIntoTasks(hashJoinProbe->prevOperator.get(), parentTask, numThreads);
    } break;
    case HASH_JOIN_BUILD: {
        auto hashJoinBuild = reinterpret_cast<HashJoinBuild*>(op);
        auto childTask =
            make_unique<ProcessorTask>(reinterpret_cast<Sink*>(hashJoinBuild), numThreads);
        decomposePlanIntoTasks(hashJoinBuild->prevOperator.get(), childTask.get(), numThreads);
        parentTask->addChildTask(move(childTask));
    } break;
    case AGGREGATE: {
        auto aggregate = reinterpret_cast<SimpleAggregate*>(op);
        auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(aggregate), numThreads);
        decomposePlanIntoTasks(aggregate->prevOperator.get(), childTask.get(), numThreads);
        parentTask->addChildTask(move(childTask));
    } break;
    case SCAN:
    case LOAD_CSV:
        break;
    default:
        decomposePlanIntoTasks(op->prevOperator.get(), parentTask, numThreads);
        break;
    }
}

} // namespace processor
} // namespace graphflow
