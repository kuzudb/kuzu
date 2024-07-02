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

    inline void setSharedStateInitialized() { sharedStateInitialized = true; }

    /*
     * This needs to be done carefully since the sink operator is backed by a unique_ptr and the
     * task is a shared_ptr, meaning the *sink* may get freed before the task. Hence, check the
     * operator in case it is NULL.
     * Also check if task has been completed, if yes return 0.
     */
    inline uint64_t getWork() override {
        common::lock_t lck{mtx};
        if (!sink) {
            return 0u;
        }
        if ((numThreadsRegistered > 0) && (numThreadsFinished == numThreadsRegistered)) {
            return 0u;
        }
        return sink->getWork() / numThreadsRegistered;
    }

    inline Sink* getSink() { return sink; }

private:
    static std::unique_ptr<ResultSet> populateResultSet(Sink* op,
        storage::MemoryManager* memoryManager);

private:
    bool sharedStateInitialized;
    Sink* sink;
    ExecutionContext* executionContext;
};

} // namespace processor
} // namespace kuzu
