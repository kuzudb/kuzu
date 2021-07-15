#include "src/storage/include/store/nodes_store.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager, bool isInMemoryMode)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")} {
    logger->info("Initializing NodesStore.");
    initStructuredPropertyColumnsAndUnstructuredPropertyLists(
        catalog, numNodesPerLabel, directory, bufferManager, isInMemoryMode);
    logger->info("Done NodesStore.");
}

void NodesStore::initStructuredPropertyColumnsAndUnstructuredPropertyLists(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    bool isInMemoryMode) {
    auto numNodeLabels = catalog.getNodeLabelsCount();
    propertyColumns.resize(numNodeLabels);
    unstrPropertyLists.resize(numNodeLabels);
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        auto& properties = catalog.getNodeProperties(nodeLabel);
        propertyColumns[nodeLabel].resize(properties.size());
        for (auto& property : properties) {
            auto propertyId = property.id;
            auto fName = getNodePropertyColumnFName(directory, nodeLabel, property.name);
            logger->debug("nodeLabel {} propertyId {} type {} name `{}`", nodeLabel, property.id,
                property.dataType, property.name);
            switch (property.dataType) {
            case INT64:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnInt64>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager, isInMemoryMode);
                break;
            case DOUBLE:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnDouble>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager, isInMemoryMode);
                break;
            case BOOL:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnBool>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager, isInMemoryMode);
                break;
            case STRING:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnString>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager, isInMemoryMode);
                break;
            case UNSTRUCTURED:
                unstrPropertyLists[nodeLabel] = make_unique<UnstructuredPropertyLists>(
                    getNodeUnstrPropertyListsFName(directory, nodeLabel), bufferManager,
                    isInMemoryMode);
                break;
            case DATE:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnDate>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager, isInMemoryMode);
                break;
            default:
                throw invalid_argument("invalid type for property column and lists creation.");
            }
        }
    }
}

} // namespace storage
} // namespace graphflow
