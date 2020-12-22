#include "src/storage/include/stores/nodes_store.h"

#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    propertyColumns.resize(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        auto& propertyMap = catalog.getPropertyMapForNodeLabel(nodeLabel);
        for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
            auto fname = getNodePropertyColumnFname(directory, nodeLabel, property->first);
            auto idx = property->second.idx;
            switch (property->second.dataType) {
            case INT:
                propertyColumns[nodeLabel][idx] = make_unique<PropertyColumnInt>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case DOUBLE:
                propertyColumns[nodeLabel][idx] = make_unique<PropertyColumnDouble>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case BOOL:
                propertyColumns[nodeLabel][idx] = make_unique<PropertyColumnBool>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case STRING:
                throw std::invalid_argument("not supported.");
                propertyColumns[nodeLabel][idx] = nullptr;
                break;
            }
        }
    }
}

} // namespace storage
} // namespace graphflow
