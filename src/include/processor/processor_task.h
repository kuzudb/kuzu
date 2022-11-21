#pragma once

#include "common/task_system/task.h"
#include "processor/operator/sink.h"

using namespace std;

namespace kuzu {
namespace processor {

class ProcessorTask : public Task {

public:
    ProcessorTask(Sink* sinkOp, ExecutionContext* executionContext)
        : Task{executionContext->numThreads}, sinkOp{sinkOp}, executionContext{executionContext} {}

    void run() override;
    void finalizeIfNecessary() override;

private:
    Sink* sinkOp;
    ExecutionContext* executionContext;
};

} // namespace processor
} // namespace kuzu
