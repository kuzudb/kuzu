#include "processor/operator/copy/copy_rel.h"

#include "storage/copier/rel_copier.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

uint64_t CopyRel::executeInternal(
    kuzu::common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto relCSVCopier = make_unique<RelCopier>(copyDescription, wal->getDirectory(), *taskScheduler,
        *catalog, nodesStore, executionContext->bufferManager, tableID, relsStatistics);
    auto numRelsCopied = relCSVCopier->copy();
    wal->logCopyRelRecord(tableID);
    return numRelsCopied;
}

} // namespace processor
} // namespace kuzu
