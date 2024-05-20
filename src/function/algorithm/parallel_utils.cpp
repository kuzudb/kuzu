#include "function/algorithm/parallel_utils.h"

#include "processor/processor_task.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace graph {

bool ParallelUtils::init() {
    std::lock_guard<std::mutex> lck(mtx);
    if (!registerMaster) {
        registerMaster = true;
        return true;
    }
    return false;
}

void ParallelUtils::mergeResults(kuzu::processor::FactorizedTable* factorizedTable) {
    std::lock_guard<std::mutex> lck(mtx);
    globalFTable->merge(*factorizedTable);
}

void ParallelUtils::doParallel(Sink *sink, ExecutionContext* executionContext) {
    auto taskScheduler = executionContext->clientContext->getTaskScheduler();
    auto parallelUtilsTask = std::make_shared<ProcessorTask>(sink, executionContext);
    parallelUtilsTask->setSharedStateInitialized();
    taskScheduler->scheduleTaskAndWaitOrError(parallelUtilsTask, executionContext);
}

} // namespace graph
} // namespace kuzu
