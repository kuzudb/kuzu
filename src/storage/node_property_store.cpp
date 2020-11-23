#include "src/storage/include/node_property_store.h"

namespace graphflow {
namespace storage {

NodePropertyStore::NodePropertyStore(Catalog& catalog, vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    columns.resize(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        auto& propertyMap = catalog.getPropertyMapForNodeLabel(nodeLabel);
        for (auto i = 0; i < propertyMap.size(); i++) {
            auto& property = propertyMap[i];
            auto fname = getNodePropertyColumnFname(directory, nodeLabel, property.propertyName);
            switch (property.dataType) {
            case INT:
                columns[nodeLabel][i] = make_unique<ColumnInteger>(
                    property.propertyName, fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case DOUBLE:
                columns[nodeLabel][i] = make_unique<ColumnDouble>(
                    property.propertyName, fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case BOOLEAN:
                columns[nodeLabel][i] = make_unique<ColumnBoolean>(
                    property.propertyName, fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            default:
                throw std::invalid_argument("not supported.");
                columns[nodeLabel][i] = nullptr;
            }
        }
    }
}

} // namespace storage
} // namespace graphflow
