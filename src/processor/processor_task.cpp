#include "include/processor_task.h"

namespace graphflow {
namespace processor {

void ProcessorTask::run() {
    // We need the lock when cloning because multiple threads can be accessing to clone,
    // which is not thread safe
    lock_t lck{mtx};
    unique_ptr<PhysicalOperator> lastOp = sinkOp->clone();
    lck.unlock();
    auto& sink = (Sink&)*lastOp;
    sink.execute(executionContext);
}

void ProcessorTask::finalizeIfNecessary() {
    sinkOp->finalize(executionContext);
}

} // namespace processor
} // namespace graphflow
