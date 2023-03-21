#include "processor/processor.h"

#include "processor/operator/aggregate/base_aggregate.h"
#include "processor/operator/copy/copy.h"
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
    if (physicalPlan->isCopy()) {
        auto copy = (Copy*)physicalPlan->lastOperator.get();
        auto outputMsg = copy->execute(taskScheduler.get(), context);
        return getFactorizedTableForOutputMsg(outputMsg, context->memoryManager);
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
    case PhysicalOperatorType::DELETE_REL: {
        parentTask->setSingleThreadedTask();
    } break;
    default:
        break;
    }
}

std::shared_ptr<FactorizedTable> QueryProcessor::getFactorizedTableForOutputMsg(
    std::string& outputMsg, MemoryManager* memoryManager) {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(std::make_unique<ColumnSchema>(
        false /* flat */, 0 /* dataChunkPos */, Types::getDataTypeSize(STRING)));
    auto factorizedTable =
        std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
    auto outputMsgVector = std::make_shared<ValueVector>(STRING, memoryManager);
    auto outputMsgChunk = std::make_shared<DataChunk>(1 /* numValueVectors */);
    outputMsgChunk->insert(0 /* pos */, outputMsgVector);
    ku_string_t outputKUStr = ku_string_t();
    outputKUStr.overflowPtr = reinterpret_cast<uint64_t>(
        outputMsgVector->getOverflowBuffer().allocateSpace(outputMsg.length()));
    outputKUStr.set(outputMsg);
    outputMsgVector->setValue(0, outputKUStr);
    outputMsgVector->state->currIdx = 0;
    factorizedTable->append(std::vector<ValueVector*>{outputMsgVector.get()});
    return factorizedTable;
}

} // namespace processor
} // namespace kuzu
