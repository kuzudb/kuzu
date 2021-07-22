#include "src/storage/include/store/nodes_store.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager, bool isInMemoryMode)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")} {
    logger->info("Initializing NodesStore.");
    initStructuredPropertyColumns(
        catalog, numNodesPerLabel, directory, bufferManager, isInMemoryMode);
    initUnstructuredPropertyLists(catalog, directory, bufferManager, isInMemoryMode);
    logger->info("Done NodesStore.");
}

void NodesStore::initStructuredPropertyColumns(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    bool isInMemoryMode) {
    auto numNodeLabels = catalog.getNodeLabelsCount();
    propertyColumns.resize(numNodeLabels);
    unstrPropertyLists.resize(numNodeLabels);
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        auto& properties = catalog.getStructuredNodeProperties(nodeLabel);
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
            case DATE:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnDate>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager, isInMemoryMode);
                break;
            default:
                throw invalid_argument("invalid type for property column creation.");
            }
        }
    }
}

void NodesStore::initUnstructuredPropertyLists(const Catalog& catalog, const string& directory,
    BufferManager& bufferManager, bool isInMemoryMode) {
    unstrPropertyLists.resize(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        if (!catalog.getUnstructuredNodeProperties(nodeLabel).empty()) {
            auto fName = getNodeUnstrPropertyListsFName(directory, nodeLabel);
            unstrPropertyLists[nodeLabel] =
                make_unique<UnstructuredPropertyLists>(fName, bufferManager, isInMemoryMode);
        }
    }
}

} // namespace storage
} // namespace graphflow
