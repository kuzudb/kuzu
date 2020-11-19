#include "src/storage/include/node_property_store.h"

namespace graphflow {
namespace storage {

NodePropertyStore::NodePropertyStore(Catalog &catalog, vector<uint64_t> &numNodesPerLabel,
    const string &directory, BufferManager &bufferManager) {
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        columns.push_back(std::vector<std::unique_ptr<ColumnBase>>());
        for (auto &property : catalog.getPropertyMapForNodeLabel(nodeLabel)) {
            auto fname = getColumnFname(directory, nodeLabel, property.propertyName);
            switch (property.dataType) {
            case INT:
                columns[nodeLabel].push_back(make_unique<ColumnInteger>(
                    property.propertyName, fname, numNodesPerLabel[nodeLabel], bufferManager));
                break;
            case DOUBLE:
                columns[nodeLabel].push_back(make_unique<ColumnDouble>(
                    property.propertyName, fname, numNodesPerLabel[nodeLabel], bufferManager));
                break;
            case BOOLEAN:
                columns[nodeLabel].push_back(make_unique<ColumnBoolean>(
                    property.propertyName, fname, numNodesPerLabel[nodeLabel], bufferManager));
                break;
            default:
                throw std::invalid_argument("not supported.");
                columns[nodeLabel].push_back(nullptr);
            }
        }
    }
}

} // namespace storage
} // namespace graphflow
