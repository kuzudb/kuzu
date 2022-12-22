#include "processor/processor.h"

#include "processor/operator/aggregate/base_aggregate.h"
#include "processor/operator/copy_csv/copy_csv.h"
#include "processor/operator/ddl/ddl.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/sink.h"
#include "processor/processor_task.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

QueryProcessor::QueryProcessor(uint64_t numThreads) {
    taskScheduler = make_unique<TaskScheduler>(numThreads);
}

shared_ptr<FactorizedTable> QueryProcessor::execute(
    PhysicalPlan* physicalPlan, ExecutionContext* context) {
    if (physicalPlan->isCopyCSV()) {
        auto copyCSV = (CopyCSV*)physicalPlan->lastOperator->getChild(0);
        auto outputMsg = copyCSV->execute(taskScheduler.get(), context);
        return getFactorizedTableForOutputMsg(outputMsg, context->memoryManager);
    } else if (physicalPlan->isDDL()) {
        auto ddl = (DDL*)physicalPlan->lastOperator->getChild(0);
        auto outputMsg = ddl->execute();
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
        auto task = make_shared<ProcessorTask>(resultCollector, context);
        decomposePlanIntoTasks(lastOperator, nullptr, task.get(), context);
        taskScheduler->scheduleTaskAndWaitOrError(task);
        return resultCollector->getResultFactorizedTable();
    }
}

void QueryProcessor::decomposePlanIntoTasks(
    PhysicalOperator* op, PhysicalOperator* parent, Task* parentTask, ExecutionContext* context) {
    if (op->isSink() && parent != nullptr) {
        auto childTask = make_unique<ProcessorTask>(reinterpret_cast<Sink*>(op), context);
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
        // Index lookup should happen exactly once. We don't need to lock if index look is executed
        // in single-thread mode.
    case PhysicalOperatorType::INDEX_SCAN: {
        parentTask->setSingleThreadedTask();
    } break;
    default:
        break;
    }
}

shared_ptr<FactorizedTable> QueryProcessor::getFactorizedTableForOutputMsg(
    string& outputMsg, MemoryManager* memoryManager) {
    auto ftTableSchema = make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(make_unique<ColumnSchema>(
        false /* flat */, 0 /* dataChunkPos */, Types::getDataTypeSize(STRING)));
    auto factorizedTable = make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
    auto outputMsgVector = make_shared<ValueVector>(STRING, memoryManager);
    auto outputMsgChunk = make_shared<DataChunk>(1 /* numValueVectors */);
    outputMsgChunk->insert(0 /* pos */, outputMsgVector);
    ku_string_t outputKUStr = ku_string_t();
    outputKUStr.overflowPtr = reinterpret_cast<uint64_t>(
        outputMsgVector->getOverflowBuffer().allocateSpace(outputMsg.length()));
    outputKUStr.set(outputMsg);
    outputMsgVector->setValue(0, outputKUStr);
    outputMsgVector->state->currIdx = 0;
    factorizedTable->append(vector<shared_ptr<ValueVector>>{outputMsgVector});
    return factorizedTable;
}

} // namespace processor
} // namespace kuzu
