#include "processor/operator/copy/copy_rel.h"

#include "storage/copy_arrow/copy_rel_arrow.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

uint64_t CopyRel::copy(common::CopyDescription& copyDescription, uint64_t numThreads) {
    auto relCSVCopier = make_unique<CopyRelArrow>(copyDescription, wal->getDirectory(), *catalog,
        nodesStatistics->getMaxNodeOffsetPerTable(), bufferManager, tableID, relsStatistics,
        numThreads);
    auto numRelsCopied = relCSVCopier->copy();
    wal->logCopyRelRecord(tableID);
    return numRelsCopied;
}

} // namespace processor
} // namespace kuzu
