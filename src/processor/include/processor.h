#pragma once

#include "src/processor/include/task_system/task_queue.h"
#include "src/storage/include/memory_manager.h"

using namespace graphflow::storage;
using namespace graphflow::transaction;

namespace graphflow {
namespace processor {

class QueryProcessor {

public:
    explicit QueryProcessor(uint64_t numThreads);
    ~QueryProcessor();

    unique_ptr<QueryResult> execute(ExecutionContext& executionContext);

private:
    void run();
    void decomposePlanIntoTasks(PhysicalOperator* op, uint64_t maxNumThreads, Task* parentTask);
    void scheduleTask(Task* task);

private:
    shared_ptr<spdlog::logger> logger;
    TaskQueue queue;
    bool stopThreads{false};
    vector<thread> threads;
    unique_ptr<MemoryManager> memManager;
};

} // namespace processor
} // namespace graphflow
