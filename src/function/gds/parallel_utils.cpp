#include "function/gds/parallel_utils.h"

#include "processor/processor_task.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

ParallelUtils::ParallelUtils(uint32_t operatorID_, common::TaskScheduler* scheduler) {
    taskScheduler = scheduler;
    operatorID = operatorID_;
}

void ParallelUtils::doParallel(ExecutionContext* executionContext, GDSAlgorithm* gdsAlgorithm,
    GDSCallSharedState *gdsCallSharedState, gds_algofunc_t gdsAlgoFunc) {
    auto parallelUtilsOp = std::make_unique<ParallelUtilsOperator>(gdsAlgorithm->copy(),
        gdsAlgoFunc, gdsCallSharedState, operatorID, "");
    auto task = std::make_shared<ProcessorTask>(parallelUtilsOp.get(), executionContext);
    task->setSharedStateInitialized();
    taskScheduler->scheduleTaskAndWaitOrError(task, executionContext);
}

} // namespace function
} // namespace kuzu
