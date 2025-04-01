#include "function/gds/gds_utils_mem.h"

#include "common/task_system/task_scheduler.h"
#include "function/gds/gds_task.h"

using namespace kuzu::processor;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

void InMemVertexComputeTask::run() {
    FrontierMorsel morsel;
    auto localVc = vc.copy();
    while (sharedState->morselDispatcher.getNextRangeMorsel(morsel)) {
        localVc->vertexCompute(morsel.getBeginOffset(), morsel.getEndOffset());
    }
}

void GDSUtilsInMemory::runVertexCompute(InMemVertexCompute& vc, InMemoryGraph& graph,
    ExecutionContext* context) {
    auto maxOffset = graph.numNodes;
    auto maxThreads = context->clientContext->getMaxNumThreadForExec();
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads);
    auto task = std::make_shared<InMemVertexComputeTask>(maxThreads, vc, sharedState);
    sharedState->morselDispatcher.init(maxOffset);
    context->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, context,
        true /* launchNewWorkerThread */);
}

} // namespace function
} // namespace kuzu
