#include "storage/store/nodes_store.h"

namespace kuzu {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, BufferManager& bufferManager, WAL* wal)
    : nodesStatisticsAndDeletedIDs{wal->getDirectory()} {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTables[tableIDSchema.first] = make_unique<NodeTable>(
            &nodesStatisticsAndDeletedIDs, bufferManager, wal, tableIDSchema.second.get());
    }
}

} // namespace storage
} // namespace kuzu
