#include "processor/processor_task.h"

#include "main/settings.h"
#include "processor/execution_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

ProcessorTask::ProcessorTask(Sink* sink, ExecutionContext* executionContext)
    : Task{executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
               .getValue<uint64_t>()},
      sharedStateInitialized{false}, sink{sink}, executionContext{executionContext} {}

void ProcessorTask::run() {
    // We need the lock when cloning because multiple threads can be accessing to clone,
    // which is not thread safe
    lock_t lck{taskMtx};
    if (!sharedStateInitialized) {
        sink->initGlobalState(executionContext);
        sharedStateInitialized = true;
    }
    auto taskRoot = sink->copy();
    lck.unlock();
    auto resultSet = sink->getResultSet(executionContext->clientContext->getMemoryManager());
    taskRoot->ptrCast<Sink>()->execute(resultSet.get(), executionContext);
}

void ProcessorTask::finalizeIfNecessary() {
    executionContext->clientContext->getProgressBar()->finishPipeline(executionContext->queryID);
    sink->finalize(executionContext);
}

} // namespace processor
} // namespace kuzu
