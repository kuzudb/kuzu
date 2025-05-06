#include "common/in_mem_gds_utils.h"

#include "common/exception/interrupt.h"
#include "common/task_system/task_scheduler.h"
#include "function/gds/gds_task.h"

using namespace kuzu::processor;
using namespace kuzu::graph;
using namespace kuzu::function;

namespace kuzu {
namespace algo_extension {

void InMemVertexComputeTask::run() {
    FrontierMorsel morsel;
    const auto localVc = vc.copy();
    while (sharedState->morselDispatcher.getNextRangeMorsel(morsel)) {
        localVc->vertexCompute(morsel.getBeginOffset(), morsel.getEndOffset());
    }
}

void InMemGDSUtils::runVertexCompute(InMemVertexCompute& vc, common::offset_t maxOffset,
    ExecutionContext* context) {
    if (context->clientContext->interrupted()) {
        throw common::InterruptException();
    }
    auto maxThreads = context->clientContext->getMaxNumThreadForExec();
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads);
    const auto task = std::make_shared<InMemVertexComputeTask>(maxThreads, vc, sharedState);
    sharedState->morselDispatcher.init(maxOffset);
    context->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, context,
        true /* launchNewWorkerThread */);
}

} // namespace algo_extension
} // namespace kuzu
