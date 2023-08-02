#include "storage/store/nodes_store.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

NodesStore::NodesStore(const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal)
    : nodesStatisticsAndDeletedIDs{wal->getDirectory()}, wal{wal} {
    for (auto nodeTableSchema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTables.emplace(
            nodeTableSchema->tableID, std::make_unique<NodeTable>(&nodesStatisticsAndDeletedIDs,
                                          bufferManager, wal, nodeTableSchema));
    }
}

} // namespace storage
} // namespace kuzu
