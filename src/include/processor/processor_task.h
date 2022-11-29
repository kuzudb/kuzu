#pragma once

#include "common/task_system/task.h"
#include "processor/operator/sink.h"

using namespace std;

namespace kuzu {
namespace processor {

class ProcessorTask : public Task {
public:
    ProcessorTask(Sink* sink, ExecutionContext* executionContext)
        : Task{executionContext->numThreads}, sink{sink}, executionContext{executionContext} {}

    void run() override;
    void finalizeIfNecessary() override;

private:
    Sink* sink;
    ExecutionContext* executionContext;
};

} // namespace processor
} // namespace kuzu
