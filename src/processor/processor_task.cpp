#include "processor/processor_task.h"

namespace kuzu {
namespace processor {

void ProcessorTask::run() {
    // We need the lock when cloning because multiple threads can be accessing to clone,
    // which is not thread safe
    lock_t lck{mtx};
    auto clonedPipeline = sink->clone();
    lck.unlock();
    auto clonedSink = (Sink*)clonedPipeline.get();
    clonedSink->execute(executionContext);
}

void ProcessorTask::finalizeIfNecessary() {
    sink->finalize(executionContext);
}

} // namespace processor
} // namespace kuzu
