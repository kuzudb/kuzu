#include "function/gds/parallel_utils.h"

#include "processor/processor_task.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

ParallelUtils::ParallelUtils(GDSCallInfo info,
    std::shared_ptr<GDSCallSharedState> sharedState,
    std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t operatorID,
    common::TaskScheduler* scheduler, std::string expressions) {
    taskScheduler = scheduler;
    gdsCallWorker = std::make_unique<GDSCallWorker>(std::move(info), sharedState,
        std::move(resultSetDescriptor), operatorID, expressions);
}

void ParallelUtils::doParallel(ExecutionContext* executionContext, gds_algofunc_t gdsAlgoFunc) {
    gdsCallWorker->setFuncToExecute(gdsAlgoFunc);
    auto task = std::make_shared<ProcessorTask>(gdsCallWorker.get(), executionContext);
    task->setSharedStateInitialized();
    taskScheduler->scheduleTaskAndWaitOrError(task, executionContext);
}

} // namespace graph
} // namespace kuzu
