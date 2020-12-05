#include "src/storage/include/stores/nodes_store.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(Catalog& catalog, vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    propertyColumns.resize(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        auto& propertyMap = catalog.getPropertyMapForNodeLabel(nodeLabel);
        for (auto i = 0u; i < propertyMap.size(); i++) {
            auto& property = propertyMap[i];
            auto fname = getNodePropertyColumnFname(directory, nodeLabel, property.name);
            switch (property.dataType) {
            case INT:
                propertyColumns[nodeLabel][i] = make_unique<PropertyColumnInt>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case DOUBLE:
                propertyColumns[nodeLabel][i] = make_unique<PropertyColumnDouble>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case BOOL:
                propertyColumns[nodeLabel][i] = make_unique<PropertyColumnBool>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case STRING:
                throw std::invalid_argument("not supported.");
                propertyColumns[nodeLabel][i] = nullptr;
                break;
            default:
                propertyColumns[nodeLabel][i] = nullptr;
            }
        }
    }
}

} // namespace storage
} // namespace graphflow
