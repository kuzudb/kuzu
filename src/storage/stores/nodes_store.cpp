#include "src/storage/include/stores/nodes_store.h"

#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    propertyColumns.resize(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        auto& propertyMap = catalog.getPropertyMapForNodeLabel(nodeLabel);
        for (auto i = 0u; i < propertyMap.size(); i++) {
            auto& property = propertyMap[i];
            auto fname = getNodePropertyColumnFname(directory, nodeLabel, property.name);
            auto size = getDataTypeSize(property.dataType);
            propertyColumns[nodeLabel][i] =
                make_unique<Column>(fname, size, numNodesPerLabel[nodeLabel], bufferManager);
        }
    }
}

} // namespace storage
} // namespace graphflow
