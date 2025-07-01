#include "processor/processor.h"

#include "common/task_system/progress_bar.h"
#include "processor/operator/sink.h"
#include "processor/processor_task.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

QueryProcessor::QueryProcessor(uint64_t numThreads) {
    taskScheduler = std::make_unique<TaskScheduler>(numThreads);
}

std::shared_ptr<FactorizedTable> QueryProcessor::execute(PhysicalPlan* physicalPlan,
    ExecutionContext* context) {
    auto lastOperator = physicalPlan->lastOperator.get();
    // The root pipeline(task) consists of operators and its prevOperator only, because we
    // expect to have linear plans. For binary operators, e.g., HashJoin, we  keep probe and its
    // prevOperator in the same pipeline, and decompose build and its prevOperator into another
    // one.
    auto sink = lastOperator->ptrCast<Sink>();
    auto task = std::make_shared<ProcessorTask>(sink, context);
    for (auto i = (int64_t)sink->getNumChildren() - 1; i >= 0; --i) {
        decomposePlanIntoTask(sink->getChild(i), task.get(), context);
    }
    initTask(task.get());
    context->clientContext->getProgressBar()->startProgress(context->queryID);
    taskScheduler->scheduleTaskAndWaitOrError(task, context);
    context->clientContext->getProgressBar()->endProgress(context->queryID);
    return sink->getResultFTable();
}

void QueryProcessor::decomposePlanIntoTask(PhysicalOperator* op, Task* task,
    ExecutionContext* context) {
    if (op->isSource()) {
        context->clientContext->getProgressBar()->addPipeline();
    }
    if (op->isSink()) {
        auto childTask = std::make_unique<ProcessorTask>(ku_dynamic_cast<Sink*>(op), context);
        for (auto i = (int64_t)op->getNumChildren() - 1; i >= 0; --i) {
            decomposePlanIntoTask(op->getChild(i), childTask.get(), context);
        }
        task->addChildTask(std::move(childTask));
    } else {
        // Schedule the right most side (e.g., build side of the hash join) first.
        for (auto i = (int64_t)op->getNumChildren() - 1; i >= 0; --i) {
            decomposePlanIntoTask(op->getChild(i), task, context);
        }
    }
}

void QueryProcessor::initTask(Task* task) {
    auto processorTask = ku_dynamic_cast<ProcessorTask*>(task);
    PhysicalOperator* op = processorTask->sink;
    while (!op->isSource()) {
        if (!op->isParallel()) {
            task->setSingleThreadedTask();
        }
        op = op->getChild(0);
    }
    if (!op->isParallel()) {
        task->setSingleThreadedTask();
    }
    for (auto& child : task->children) {
        initTask(child.get());
    }
}

} // namespace processor
} // namespace kuzu
