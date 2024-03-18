#pragma once

#include "common/task_system/task.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

class ProcessorTask : public common::Task {
    friend class QueryProcessor;

public:
    ProcessorTask(Sink* sink, ExecutionContext* executionContext);

    void run() override;
    void finalizeIfNecessary() override;
    void addSource(PhysicalOperator* source);

private:
    static std::unique_ptr<ResultSet> populateResultSet(
        Sink* op, storage::MemoryManager* memoryManager);

    void updateProgress() const override;

private:
    bool sharedStateInitialized;
    Sink* sink;
    std::vector<PhysicalOperator*> sourceOps;
    ExecutionContext* executionContext;
};

} // namespace processor
} // namespace kuzu
