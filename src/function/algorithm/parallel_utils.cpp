#include "function/algorithm/parallel_utils.h"

#include "processor/processor_task.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace graph {

void ParallelUtils::doParallel(ExecutionContext* executionContext) {
    auto taskScheduler = executionContext->clientContext->getTaskScheduler();
    auto parallelUtilsTask =
        std::make_shared<ProcessorTask>(algorithmRunnerWorker.get(), executionContext);
    parallelUtilsTask->setSharedStateInitialized();
    taskScheduler->scheduleTaskAndWaitOrError(parallelUtilsTask, executionContext);
}

} // namespace graph
} // namespace kuzu
