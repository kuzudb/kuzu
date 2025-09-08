#include "common/in_mem_gds_utils.h"

#include "common/exception/interrupt.h"
#include "common/task_system/task_scheduler.h"
#include "common/types/types.h"
#include "function/gds/gds_task.h"
#include "main/client_context.h"

using namespace kuzu::processor;
using namespace kuzu::graph;
using namespace kuzu::function;

namespace kuzu {
namespace algo_extension {

void InMemParallelComputeTask::run() {
    FrontierMorsel morsel;
    const auto localVc = vc.copy();
    while (sharedState->morselDispatcher.getNextRangeMorsel(morsel)) {
        localVc->parallelCompute(morsel.getBeginOffset(), morsel.getEndOffset(), tableId);
    }
}

void InMemGDSUtils::runParallelCompute(InMemParallelCompute& vc, common::offset_t maxOffset,
    ExecutionContext* context, std::optional<common::table_id_t> tableId) {
    auto clientContext = context->clientContext;
    if (clientContext->interrupted()) {
        throw common::InterruptException();
    }
    auto maxThreads = clientContext->getMaxNumThreadForExec();
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads);
    const auto task =
        std::make_shared<InMemParallelComputeTask>(maxThreads, vc, sharedState, tableId);
    sharedState->morselDispatcher.init(maxOffset);
    common::TaskScheduler::Get(*clientContext)
        ->scheduleTaskAndWaitOrError(task, context, true /* launchNewWorkerThread */);
}
} // namespace algo_extension
} // namespace kuzu
