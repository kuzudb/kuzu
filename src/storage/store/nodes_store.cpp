#include "storage/store/nodes_store.h"

namespace kuzu {
namespace storage {

NodesStore::NodesStore(
    BMFileHandle* dataFH, const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal)
    : nodesStatisticsAndDeletedIDs{wal->getDirectory()}, wal{wal}, dataFH{dataFH},
      metadataFH{catalog.getMetadataFH()} {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTables[tableIDSchema.first] =
            std::make_unique<NodeTable>(dataFH, catalog.getMetadataFH(),
                &nodesStatisticsAndDeletedIDs, bufferManager, wal, tableIDSchema.second.get());
    }
}

} // namespace storage
} // namespace kuzu
