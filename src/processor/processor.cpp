#include "processor/processor.h"

#include "processor/operator/aggregate/base_aggregate.h"
#include "processor/operator/copy/copy.h"
#include "processor/operator/copy/copy_node.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/sink.h"
#include "processor/processor_task.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

QueryProcessor::QueryProcessor(uint64_t numThreads) {
    taskScheduler = std::make_unique<TaskScheduler>(numThreads);
}

std::shared_ptr<FactorizedTable> QueryProcessor::execute(
    PhysicalPlan* physicalPlan, ExecutionContext* context) {
    if (physicalPlan->isCopyRelOrNPY()) {
        auto copy = (Copy*)physicalPlan->lastOperator.get();
        auto outputMsg = copy->execute(taskScheduler.get(), context);
        return FactorizedTableUtils::getFactorizedTableForOutputMsg(
            outputMsg, context->memoryManager);
    } else {
        auto lastOperator = physicalPlan->lastOperator.get();
        // Init global state before decompose into pipelines. Otherwise, each pipeline will try to
        // init global state. Result in global state being initialized multiple times.
        lastOperator->initGlobalState(context);
        auto resultCollector = reinterpret_cast<ResultCollector*>(lastOperator);
        // The root pipeline(task) consists of operators and its prevOperator only, because we
        // expect to have linear plans. For binary operators, e.g., HashJoin, we  keep probe and its
        // prevOperator in the same pipeline, and decompose build and its prevOperator into another
        // one.
        auto task = std::make_shared<ProcessorTask>(resultCollector, context);
        decomposePlanIntoTasks(lastOperator, nullptr, task.get(), context);
        taskScheduler->scheduleTaskAndWaitOrError(task, context);
        return resultCollector->getResultFactorizedTable();
    }
}

void QueryProcessor::decomposePlanIntoTasks(
    PhysicalOperator* op, PhysicalOperator* parent, Task* parentTask, ExecutionContext* context) {
    if (op->isSink() && parent != nullptr) {
        auto childTask = std::make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), context);
        if (op->getOperatorType() == PhysicalOperatorType::AGGREGATE) {
            auto aggregate = (BaseAggregate*)op;
            if (aggregate->containDistinctAggregate()) {
                // Distinct aggregate should be executed in single-thread mode.
                childTask->setSingleThreadedTask();
            }
        }
        decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), context);
        parentTask->addChildTask(std::move(childTask));
    } else {
        // Schedule the right most side (e.g., build side of the hash join) first.
        for (auto i = (int64_t)op->getNumChildren() - 1; i >= 0; --i) {
            decomposePlanIntoTasks(op->getChild(i), op, parentTask, context);
        }
    }
    switch (op->getOperatorType()) {
        // Ordered table should be scanned in single-thread mode.
    case PhysicalOperatorType::ORDER_BY_MERGE:
        // DDL should be executed exactly once.
    case PhysicalOperatorType::CREATE_NODE_TABLE:
    case PhysicalOperatorType::CREATE_REL_TABLE:
    case PhysicalOperatorType::DROP_TABLE:
    case PhysicalOperatorType::DROP_PROPERTY:
    case PhysicalOperatorType::ADD_PROPERTY:
    case PhysicalOperatorType::RENAME_PROPERTY:
    case PhysicalOperatorType::RENAME_TABLE:
        // As a temporary solution, update is executed in single thread mode.
    case PhysicalOperatorType::SET_NODE_PROPERTY:
    case PhysicalOperatorType::SET_REL_PROPERTY:
    case PhysicalOperatorType::CREATE_NODE:
    case PhysicalOperatorType::CREATE_REL:
    case PhysicalOperatorType::DELETE_NODE:
    case PhysicalOperatorType::DELETE_REL:
    case PhysicalOperatorType::CALL: {
        parentTask->setSingleThreadedTask();
    } break;
    default:
        break;
    }
}

} // namespace processor
} // namespace kuzu
