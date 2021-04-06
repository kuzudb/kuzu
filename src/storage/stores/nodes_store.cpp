#include "src/storage/include/stores/nodes_store.h"

#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager)
    : logger{spdlog::get("storage")} {
    logger->debug("Initializing NodesStore.");
    propertyColumns.resize(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        auto& propertyMap = catalog.getPropertyMapForNodeLabel(nodeLabel);
        propertyColumns[nodeLabel].resize(propertyMap.size());
        for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
            auto fname = getNodePropertyColumnFname(directory, nodeLabel, property->first);
            auto idx = property->second.idx;
            logger->debug("nodeLabel {} propertyIdx {} type {} name `{}`", nodeLabel, idx,
                property->second.dataType, property->first);
            switch (property->second.dataType) {
            case INT32:
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
                propertyColumns[nodeLabel][idx] = make_unique<PropertyColumnString>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            default:
                throw invalid_argument("invalid type for property column craetion.");
            }
        }
    }
    logger->debug("Done.");
}

} // namespace storage
} // namespace graphflow
