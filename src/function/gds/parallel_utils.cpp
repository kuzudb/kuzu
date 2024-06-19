#include "function/gds/parallel_utils.h"

#include "function/gds/gds_task.h"
#include "main/settings.h"
// TODO(Semih): Remove
#include <iostream>
/*
#include "processor/operator/gds_parallelizer.h"
#include "processor/result/factorized_table.h"
*/

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

ParallelUtils::ParallelUtils(common::TaskScheduler* taskScheduler, uint32_t operatorID)
    : taskScheduler{taskScheduler}, operatorID{operatorID} {}
//    gdsCallWorker = std::make_unique<GDSCallWorker>(std::move(info), sharedState,
//        std::move(resultSetDescriptor), operatorID, expressions);
// }

void ParallelUtils::countParallel(ExecutionContext* executionContext, int64_t count) {
    auto sharedState = std::make_shared<GDSTaskSharedState>(count);
    //    auto gdsParallelizer = std::make_unique<GDSParallelizer>(sharedState, operatorID);
    auto task = std::make_shared<GDSTask>(
        executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
            .getValue<uint64_t>(),
        sharedState);
    std::cout << "countParallel maxNumberOfThreads: "
              << executionContext->clientContext->getMaxNumThreadForExec() << std::endl;
    taskScheduler->scheduleTaskAndWaitOrError(task, executionContext);
}

// void ParallelUtils::doParallel(ExecutionContext* executionContext, gds_algofunc_t gdsAlgoFunc) {
//     gdsCallWorker->setFuncToExecute(gdsAlgoFunc);
//     auto task = std::make_shared<ProcessorTask>(gdsCallWorker.get(), executionContext);
//     task->setSharedStateInitialized();
//     taskScheduler->scheduleTaskAndWaitOrError(task, executionContext);
// }

} // namespace function
} // namespace kuzu