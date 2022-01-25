#pragma once

#include "src/processor/include/processor_task.h"

#include "src/processor/include/physical_plan/operator/result_collector.h"

namespace graphflow {
namespace processor {

ProcessorTask::ProcessorTask(Sink* sinkOp, uint64_t maxNumThreads)
    : Task{maxNumThreads}, sinkOp{sinkOp} {}

void ProcessorTask::run() {
    // We need the lock when cloning because multiple threads can be accessing to clone,
    // which is not thread safe
    lock_t lck{mtx};
    unique_ptr<PhysicalOperator> lastOp = sinkOp->clone();
    lck.unlock();
    auto& sink = (Sink&)*lastOp;
    sink.execute();
}

void ProcessorTask::finalizeIfNecessary() {
    sinkOp->finalize();
}

} // namespace processor
} // namespace graphflow
