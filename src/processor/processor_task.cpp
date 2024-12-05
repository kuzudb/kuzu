#include "processor/processor_task.h"

#include "main/settings.h"

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
    auto clonedPipelineRoot = sink->clone();
    lck.unlock();
    auto currentSink = (Sink*)clonedPipelineRoot.get();
    auto resultSet =
        populateResultSet(currentSink, executionContext->clientContext->getMemoryManager());
    currentSink->execute(resultSet.get(), executionContext);
}

void ProcessorTask::finalizeIfNecessary() {
    executionContext->clientContext->getProgressBar()->finishPipeline(executionContext->queryID);
    sink->finalize(executionContext);
}

std::unique_ptr<ResultSet> ProcessorTask::populateResultSet(Sink* op,
    storage::MemoryManager* memoryManager) {
    auto resultSetDescriptor = op->getResultSetDescriptor();
    if (resultSetDescriptor == nullptr) {
        // Some pipeline does not need a resultSet, e.g. OrderByMerge
        return nullptr;
    }
    return std::make_unique<ResultSet>(resultSetDescriptor, memoryManager);
}

} // namespace processor
} // namespace kuzu
