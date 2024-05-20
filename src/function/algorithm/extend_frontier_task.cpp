#include "function/algorithm/task/extend_frontier_task.h"

namespace kuzu {
namespace graph {

void ExtendFrontierTask::run() {

    populateResultSet(executionContext->clientContext->getMemoryManager());
}

std::unique_ptr<processor::ResultSet> ExtendFrontierTask::populateResultSet(
    storage::MemoryManager* memoryManager) {
    return std::make_unique<processor::ResultSet>(resultSetDescriptor, memoryManager);
}

} // namespace graph
} // namespace kuzu
