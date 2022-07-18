#include "src/storage/store/include/nodes_store.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, BufferManager& bufferManager,
    const string& directory, bool isInMemoryMode, WAL* wal)
    : nodesMetadata(directory) {
    nodeTables.resize(catalog.getReadOnlyVersion()->getNumNodeLabels());
    for (auto label = 0u; label < catalog.getReadOnlyVersion()->getNumNodeLabels(); label++) {
        nodeTables[label] = make_unique<NodeTable>(nodesMetadata.getNodeMetadata(label),
            bufferManager, isInMemoryMode,
            catalog.getReadOnlyVersion()->getAllNodeProperties(label), directory, wal);
    }
}

} // namespace storage
} // namespace graphflow
