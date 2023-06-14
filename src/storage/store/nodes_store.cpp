#include "storage/store/nodes_store.h"

namespace kuzu {
namespace storage {

NodesStore::NodesStore(BMFileHandle* nodeGroupsDataFH, BMFileHandle* nodeGroupsMetaFH,
    const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal)
    : nodesStatisticsAndDeletedIDs{wal->getDirectory()}, wal{wal},
      nodeGroupsDataFH{nodeGroupsDataFH}, nodeGroupsMetaFH{nodeGroupsMetaFH} {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTables[tableIDSchema.first] =
            std::make_unique<NodeTable>(nodeGroupsDataFH, nodeGroupsMetaFH,
                &nodesStatisticsAndDeletedIDs, bufferManager, wal, tableIDSchema.second.get());
    }
}

} // namespace storage
} // namespace kuzu
