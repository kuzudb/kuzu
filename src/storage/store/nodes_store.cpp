#include "storage/store/nodes_store.h"

namespace kuzu {
namespace storage {

NodesStore::NodesStore(const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal)
    : nodesStatisticsAndDeletedIDs{wal->getDirectory()}, wal{wal} {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTables[tableIDSchema.first] = std::make_unique<NodeTable>(
            &nodesStatisticsAndDeletedIDs, bufferManager, wal, tableIDSchema.second.get());
    }
}

} // namespace storage
} // namespace kuzu
