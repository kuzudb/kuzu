#include "processor/processor_task.h"

namespace kuzu {
namespace processor {

void ProcessorTask::run() {
    // We need the lock when cloning because multiple threads can be accessing to clone,
    // which is not thread safe
    common::lock_t lck{mtx};
    auto clonedPipelineRoot = sink->clone();
    lck.unlock();
    auto currentSink = (Sink*)clonedPipelineRoot.get();
    auto resultSet = populateResultSet(currentSink, executionContext->memoryManager);
    currentSink->execute(resultSet.get(), executionContext);
}

void ProcessorTask::finalizeIfNecessary() {
    sink->finalize(executionContext);
}

std::unique_ptr<ResultSet> ProcessorTask::populateResultSet(
    Sink* op, storage::MemoryManager* memoryManager) {
    auto resultSetDescriptor = op->getResultSetDescriptor();
    if (resultSetDescriptor == nullptr) {
        // Some pipeline does not need a resultSet, e.g. OrderByMerge
        return nullptr;
    }
    return std::make_unique<ResultSet>(resultSetDescriptor, memoryManager);
}

} // namespace processor
} // namespace kuzu
