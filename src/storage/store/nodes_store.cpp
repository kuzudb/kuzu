#include "src/storage/include/store/nodes_store.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, BufferManager& bufferManager,
    const string& directory, bool isInMemoryMode)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")} {
    nodes.resize(catalog.getNumNodeLabels());
    for (auto label = 0u; label < catalog.getNumNodeLabels(); label++) {
        nodes[label] = make_unique<Node>(
            label, bufferManager, isInMemoryMode, catalog.getAllNodeProperties(label), directory);
    }
}

} // namespace storage
} // namespace graphflow
