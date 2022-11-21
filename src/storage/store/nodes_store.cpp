#include "storage/store/nodes_store.h"

namespace kuzu {
namespace storage {

NodesStore::NodesStore(
    const Catalog& catalog, BufferManager& bufferManager, bool isInMemoryMode, WAL* wal)
    : nodesStatisticsAndDeletedIDs{wal->getDirectory()}, isInMemoryMode{isInMemoryMode} {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTables[tableIDSchema.first] = make_unique<NodeTable>(&nodesStatisticsAndDeletedIDs,
            bufferManager, isInMemoryMode, wal, tableIDSchema.second.get());
    }
}

} // namespace storage
} // namespace kuzu
