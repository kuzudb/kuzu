#include "src/storage/include/store/nodes_store.h"

#include "spdlog/spdlog.h"

#include "src/storage/include/data_structure/column.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager)
    : logger{spdlog::get("storage")} {
    logger->info("Initializing NodesStore.");
    initStructuredPropertyColumnsAndUnstructuredPropertyLists(
        catalog, numNodesPerLabel, directory, bufferManager);
    logger->info("Done NodesStore.");
}

void NodesStore::initStructuredPropertyColumnsAndUnstructuredPropertyLists(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory,
    BufferManager& bufferManager) {
    auto numNodeLabels = catalog.getNodeLabelsCount();
    propertyColumns.resize(numNodeLabels);
    unstrPropertyLists.resize(numNodeLabels);
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        auto& properties = catalog.getNodeProperties(nodeLabel);
        propertyColumns[nodeLabel].resize(properties.size());
        for (auto& property : properties) {
            auto propertyId = property.id;
            auto fName = getNodePropertyColumnFname(directory, nodeLabel, property.name);
            logger->debug("nodeLabel {} propertyId {} type {} name `{}`", nodeLabel, property.id,
                property.dataType, property.name);
            switch (property.dataType) {
            case INT64:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnInt64>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case DOUBLE:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnDouble>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case BOOL:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnBool>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case STRING:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnString>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case UNSTRUCTURED:
                unstrPropertyLists[nodeLabel] = make_unique<UnstructuredPropertyLists>(
                    getNodeUnstrPropertyListsFname(directory, nodeLabel), bufferManager);
                break;
            case DATE:
                propertyColumns[nodeLabel][propertyId] = make_unique<PropertyColumnDate>(
                    fName, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            default:
                throw invalid_argument("invalid type for property column and lists creation.");
            }
        }
    }
}

} // namespace storage
} // namespace graphflow
