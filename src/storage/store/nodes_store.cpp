#include "storage/store/nodes_store.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

NodesStore::NodesStore(BMFileHandle* dataFH, BMFileHandle* metadataFH, const Catalog& catalog,
    BufferManager& bufferManager, WAL* wal)
    : nodesStatisticsAndDeletedIDs{wal->getDirectory()}, wal{wal}, dataFH{dataFH}, metadataFH{
                                                                                       metadataFH} {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTables[tableIDSchema->tableID] = std::make_unique<NodeTable>(
            dataFH, metadataFH, &nodesStatisticsAndDeletedIDs, bufferManager, wal, tableIDSchema);
    }
}

} // namespace storage
} // namespace kuzu
