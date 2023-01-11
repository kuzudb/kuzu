#include "processor/operator/copy_csv/copy_rel_csv.h"

#include "storage/in_mem_csv_copier/in_mem_rel_csv_copier.h"

namespace kuzu {
namespace processor {

string CopyRelCSV::execute(TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto relCSVCopier = make_unique<InMemRelCSVCopier>(csvDescription, wal->getDirectory(),
        *taskScheduler, *catalog, nodesStatisticsAndDeletedIDs->getMaxNodeOffsetPerTable(),
        executionContext->bufferManager, tableID, relsStatistics);
    errorIfTableIsNonEmpty(relsStatistics);
    // Note: This copy function will update the numRelsPerDirectionBoundTable and numRels
    // information in relsStatistics for this relTable.
    auto numRelsCopied = relCSVCopier->copy();
    wal->logCopyRelCSVRecord(tableID);
    return StringUtils::string_format("%d number of rels has been copied to relTable: %s.",
        numRelsCopied, catalog->getReadOnlyVersion()->getTableName(tableID).c_str());
}

} // namespace processor
} // namespace kuzu
