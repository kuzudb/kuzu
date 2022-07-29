#include "src/storage/store/include/nodes_store.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(
    const Catalog& catalog, BufferManager& bufferManager, bool isInMemoryMode, WAL* wal)
    : nodesMetadata(wal->getDirectory()), isInMemoryMode{isInMemoryMode} {
    nodeTables.resize(catalog.getReadOnlyVersion()->getNumNodeLabels());
    for (auto label = 0u; label < catalog.getReadOnlyVersion()->getNumNodeLabels(); label++) {
        nodeTables[label] = make_unique<NodeTable>(&nodesMetadata, bufferManager, isInMemoryMode,
            wal, catalog.getReadOnlyVersion()->getNodeLabel(label));
    }
}

} // namespace storage
} // namespace graphflow
