#pragma once

#include "src/common/include/task_system/task.h"
#include "src/processor/include/physical_plan/operator/sink.h"

using namespace std;

namespace graphflow {
namespace processor {

class ProcessorTask : public Task {

public:
    ProcessorTask(Sink* sinkOp, uint64_t numThreads);

    void run() override;
    void finalizeIfNecessary() override;

private:
    Sink* sinkOp;
};

} // namespace processor
} // namespace graphflow
