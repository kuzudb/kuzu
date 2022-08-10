#include "src/processor/include/processor.h"

#include "src/processor/include/physical_plan/operator/aggregate/base_aggregate.h"
#include "src/processor/include/physical_plan/operator/copy_csv/copy_node_csv.h"
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
    PhysicalPlan* physicalPlan, ExecutionContext* context) {
    if (physicalPlan->lastOperator->getChild(0)->getOperatorType() != COPY_NODE_CSV) {
        auto lastOperator = physicalPlan->lastOperator.get();
        auto resultCollector = reinterpret_cast<ResultCollector*>(lastOperator);
        // The root pipeline(task) consists of operators and its prevOperator only, because we
        // expect to have linear plans. For binary operators, e.g., HashJoin, we  keep probe and its
        // prevOperator in the same pipeline, and decompose build and its prevOperator into another
        // one.
        auto task = make_shared<ProcessorTask>(resultCollector, context);
        decomposePlanIntoTasks(lastOperator, lastOperator, task.get(), context);
        taskScheduler->scheduleTaskAndWaitOrError(task);
        return resultCollector->getResultFactorizedTable();
    } else {
        auto copyCSV = (CopyNodeCSV*)physicalPlan->lastOperator->getChild(0);
        copyCSV->execute(*taskScheduler);
        return make_shared<FactorizedTable>(nullptr, make_unique<TableSchema>());
    }
}

void QueryProcessor::decomposePlanIntoTasks(
    PhysicalOperator* op, PhysicalOperator* parent, Task* parentTask, ExecutionContext* context) {
    switch (op->getOperatorType()) {
    case RESULT_COLLECTOR: {
        if (parent->getOperatorType() == UNION_ALL_SCAN ||
            parent->getOperatorType() == FACTORIZED_TABLE_SCAN) {
            auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), context);
            decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), context);
            parentTask->addChildTask(move(childTask));
        } else {
            decomposePlanIntoTasks(op->getChild(0), op, parentTask, context);
        }
    } break;
    case ORDER_BY_MERGE: {
        auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), context);
        decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), context);
        parentTask->addChildTask(move(childTask));
        parentTask->setSingleThreadedTask();
    } break;
    case ORDER_BY: {
        auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), context);
        decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), context);
        parentTask->addChildTask(move(childTask));
    } break;
    case HASH_JOIN_BUILD: {
        auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), context);
        decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), context);
        parentTask->addChildTask(move(childTask));
    } break;
    case AGGREGATE: {
        auto aggregate = (BaseAggregate*)op;
        auto childTask = make_unique<ProcessorTask>(aggregate, context);
        if (aggregate->containDistinctAggregate()) {
            childTask->setSingleThreadedTask();
        }
        decomposePlanIntoTasks(op->getChild(0), op, childTask.get(), context);
        parentTask->addChildTask(move(childTask));
    } break;
    default: {
        for (auto i = 0u; i < op->getNumChildren(); i++) {
            decomposePlanIntoTasks(op->getChild(i), op, parentTask, context);
        }
    } break;
    }
}

} // namespace processor
} // namespace graphflow
