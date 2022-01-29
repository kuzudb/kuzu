#include "src/processor/include/processor.h"

#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/processor_task.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

QueryProcessor::QueryProcessor(uint64_t numThreads) {
    taskScheduler = make_unique<TaskScheduler>(numThreads);
}

shared_ptr<FactorizedTable> QueryProcessor::execute(
    PhysicalPlan* physicalPlan, uint64_t numThreads) {
    auto lastOperator = physicalPlan->lastOperator.get();
    auto resultCollector = reinterpret_cast<ResultCollector*>(lastOperator);
    // The root pipeline(task) consists of operators and its prevOperator only, because we
    // expect to have linear plans. For binary operators, e.g., HashJoin, we  keep probe and its
    // prevOperator in the same pipeline, and decompose build and its prevOperator into another one.
    auto task = make_shared<ProcessorTask>(resultCollector, numThreads);
    decomposePlanIntoTasks(lastOperator, lastOperator, task.get(), numThreads);
    taskScheduler->scheduleTaskAndWaitOrError(task);
    return resultCollector->getResultFactorizedTable();
}

void QueryProcessor::decomposePlanIntoTasks(
    PhysicalOperator* op, PhysicalOperator* parent, Task* parentTask, uint64_t numThreads) {
    switch (op->getOperatorType()) {
    case RESULT_COLLECTOR: {
        if (parent->getOperatorType() == UNION_ALL_SCAN) {
            // Only breaks the pipeline if the parent operator is a UNION_ALL_SCAN.
            auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), numThreads);
            decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), numThreads);
            parentTask->addChildTask(move(childTask));
        } else {
            decomposePlanIntoTasks(op->getChild(0), op, parentTask, numThreads);
        }
    } break;
    case ORDER_BY_MERGE: {
        auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), numThreads);
        decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), numThreads);
        parentTask->addChildTask(move(childTask));
        parentTask->setSingleThreadedTask();
    } break;
    case ORDER_BY: {
        auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), numThreads);
        decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), numThreads);
        parentTask->addChildTask(move(childTask));
    } break;
    case HASH_JOIN_BUILD: {
        auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), numThreads);
        decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), numThreads);
        parentTask->addChildTask(move(childTask));
    } break;
    case AGGREGATE: {
        auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), numThreads);
        decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), numThreads);
        parentTask->addChildTask(move(childTask));
    } break;
    default: {
        for (auto i = 0u; i < op->getNumChildren(); i++) {
            decomposePlanIntoTasks(op->getChild(i), op, parentTask, numThreads);
        }
    } break;
    }
}

} // namespace processor
} // namespace graphflow
