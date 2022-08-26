#include "src/storage/store/include/nodes_store.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(
    const Catalog& catalog, BufferManager& bufferManager, bool isInMemoryMode, WAL* wal)
    : nodesStatisticsAndDeletedIDs{wal->getDirectory()}, isInMemoryMode{isInMemoryMode} {
    nodeTables.resize(catalog.getReadOnlyVersion()->getNumNodeTables());
    for (auto tableID = 0u; tableID < catalog.getReadOnlyVersion()->getNumNodeTables(); tableID++) {
        nodeTables[tableID] = make_unique<NodeTable>(&nodesStatisticsAndDeletedIDs, bufferManager,
            isInMemoryMode, wal, catalog.getReadOnlyVersion()->getNodeTableSchema(tableID));
    }
}

} // namespace storage
} // namespace graphflow
