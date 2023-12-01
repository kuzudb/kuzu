#pragma once

#include "common/task_system/task.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

class ProcessorTask : public common::Task {
    friend class QueryProcessor;

public:
    ProcessorTask(Sink* sink, ExecutionContext* executionContext)
        : Task{executionContext->numThreads}, sink{sink}, executionContext{executionContext} {}

    void run() override;
    void finalizeIfNecessary() override;

private:
    static std::unique_ptr<ResultSet> populateResultSet(
        Sink* op, storage::MemoryManager* memoryManager);

private:
    Sink* sink;
    ExecutionContext* executionContext;
};

} // namespace processor
} // namespace kuzu
