#include "processor/operator/copy/copy_rel.h"

#include "storage/copier/rel_copy_executor.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

uint64_t CopyRel::executeInternal(
    kuzu::common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto tableSchema = catalog->getReadOnlyVersion()->getRelTableSchema(tableID);
    auto relCopier = std::make_unique<RelCopyExecutor>(
        copyDescription, wal, *taskScheduler, nodesStore, table, tableSchema, relsStatistics);
    auto numRelsCopied = relCopier->copy(executionContext);
    return numRelsCopied;
}

} // namespace processor
} // namespace kuzu
