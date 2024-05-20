#pragma once

#include "common/task_system/task.h"
#include "main/settings.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace graph {

class ExtendFrontierTask : public common::Task {
public:
    explicit ExtendFrontierTask(processor::ExecutionContext* executionContext)
        : Task{executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
                   .getValue<uint64_t>()},
          sharedStateInitialized{false}, executionContext{executionContext} {}

    void run() override;

private:
    std::unique_ptr<processor::ResultSet> populateResultSet(storage::MemoryManager* memoryManager);

private:
    // TODO (Anurag): Add the common shared state data structures required here
    // BFSSharedState *bfsSharedState; ?
    bool sharedStateInitialized;
    processor::ResultSetDescriptor* resultSetDescriptor;
    processor::ExecutionContext* executionContext;
};

} // namespace graph
} // namespace kuzu