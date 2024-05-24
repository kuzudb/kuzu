#include "function/algorithm/parallel_utils.h"

#include "processor/operator/algorithm/algorithm_runner.h"
#include "processor/processor_task.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::function;

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

void ParallelUtils::doParallel(Sink* sink, ExecutionContext* executionContext, table_func_t func) {
    auto taskScheduler = executionContext->clientContext->getTaskScheduler();
    auto algorithmRunner = ku_dynamic_cast<Sink*, AlgorithmRunner*>(sink);
    algorithmRunner->setTableFunc(func);
    auto parallelUtilsTask = std::make_shared<ProcessorTask>(sink, executionContext);
    parallelUtilsTask->setSharedStateInitialized();
    taskScheduler->scheduleTaskAndWaitOrError(parallelUtilsTask, executionContext);
}

} // namespace graph
} // namespace kuzu
