#include "src/storage/store/include/nodes_store.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, BufferManager& bufferManager,
    const string& directory, bool isInMemoryMode, WAL* wal) {
    nodeTables.resize(catalog.getNumNodeLabels());
    for (auto label = 0u; label < catalog.getNumNodeLabels(); label++) {
        nodeTables[label] = make_unique<NodeTable>(label, bufferManager, isInMemoryMode,
            catalog.getAllNodeProperties(label), directory, wal);
    }
}

} // namespace storage
} // namespace graphflow
