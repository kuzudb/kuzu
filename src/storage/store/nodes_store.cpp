#include "storage/store/nodes_store.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

NodesStore::NodesStore(BMFileHandle* dataFH, BMFileHandle* metadataFH, const Catalog& catalog,
    BufferManager& bufferManager, WAL* wal)
    : nodesStatisticsAndDeletedIDs{wal->getDirectory()}, wal{wal}, dataFH{dataFH}, metadataFH{
                                                                                       metadataFH} {
    for (auto& schema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(schema);
        nodeTables[schema->tableID] = std::make_unique<NodeTable>(
            dataFH, metadataFH, &nodesStatisticsAndDeletedIDs, bufferManager, wal, nodeTableSchema);
    }
}

} // namespace storage
} // namespace kuzu
