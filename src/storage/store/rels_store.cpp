#include "src/storage/include/store/rels_store.h"

#include "src/common/include/compression_scheme.h"

namespace graphflow {
namespace storage {

RelsStore::RelsStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")} {
    initAdjColumns(catalog, numNodesPerLabel, directory, bufferManager);
    initAdjLists(catalog, numNodesPerLabel, directory, bufferManager);
    initPropertyListsAndColumns(catalog, numNodesPerLabel, directory, bufferManager);
}

void RelsStore::initAdjColumns(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    logger->info("Initializing AdjColumns.");
    for (auto direction : DIRECTIONS) {
        auto numNodeLabels = catalog.getNodeLabelsCount();
        auto numRelLabels = catalog.getRelLabelsCount();
        adjColumns[direction].resize(numNodeLabels);
        for (auto nodeLabel = 0u; nodeLabel < numNodeLabels; nodeLabel++) {
            adjColumns[direction][nodeLabel].resize(numRelLabels);
            for (auto relLabel : catalog.getRelLabelsForNodeLabelDirection(nodeLabel, direction)) {
                if (catalog.isSingleMultiplicityInDirection(relLabel, direction)) {
                    NodeIDCompressionScheme nodeIDCompressionScheme{
                        catalog.getNodeLabelsForRelLabelDirection(relLabel, !direction),
                        numNodesPerLabel, numNodeLabels};
                    logger->debug("DIRECTION {} nodeLabel {} relLabel {} compressionScheme {},{}",
                        direction, nodeLabel, relLabel,
                        nodeIDCompressionScheme.getNumBytesForLabel(),
                        nodeIDCompressionScheme.getNumBytesForOffset());
                    auto fName = getAdjColumnFName(directory, relLabel, nodeLabel, direction);
                    adjColumns[direction][nodeLabel][relLabel] = make_unique<AdjColumn>(
                        fName, numNodesPerLabel[nodeLabel], bufferManager, nodeIDCompressionScheme);
                }
            }
        }
    }
    logger->info("Initializing AdjColumns done.");
}

void RelsStore::initAdjLists(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    logger->info("Initializing AdjLists.");
    for (auto direction : DIRECTIONS) {
        adjLists[direction].resize(catalog.getNodeLabelsCount());
        for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
            adjLists[direction][nodeLabel].resize(catalog.getRelLabelsCount());
            for (auto relLabel : catalog.getRelLabelsForNodeLabelDirection(nodeLabel, direction)) {
                if (!catalog.isSingleMultiplicityInDirection(relLabel, direction)) {
                    NodeIDCompressionScheme nodeIDCompressionScheme{
                        catalog.getNodeLabelsForRelLabelDirection(relLabel, !direction),
                        numNodesPerLabel, catalog.getNodeLabelsCount()};
                    logger->debug("DIRECTION {} nodeLabel {} relLabel {} compressionScheme {},{}",
                        direction, nodeLabel, relLabel,
                        nodeIDCompressionScheme.getNumBytesForLabel(),
                        nodeIDCompressionScheme.getNumBytesForOffset());
                    auto fname = getAdjListsFName(directory, relLabel, nodeLabel, direction);
                    adjLists[direction][nodeLabel][relLabel] =
                        make_unique<AdjLists>(fname, bufferManager, nodeIDCompressionScheme);
                }
            }
        }
    }
    logger->info("Initializing AdjLists done.");
}

void RelsStore::initPropertyListsAndColumns(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory,
    BufferManager& bufferManager) {
    logger->info("Initializing PropertyLists and PropertyColumns.");
    propertyColumns.resize(catalog.getNodeLabelsCount());
    propertyLists[FWD].resize(catalog.getNodeLabelsCount());
    propertyLists[BWD].resize(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        propertyColumns[nodeLabel].resize(catalog.getRelLabelsCount());
        propertyLists[FWD][nodeLabel].resize(catalog.getRelLabelsCount());
        propertyLists[BWD][nodeLabel].resize(catalog.getRelLabelsCount());
    }
    for (auto relLabel = 0u; relLabel < catalog.getRelLabelsCount(); relLabel++) {
        if (!catalog.getRelProperties(relLabel).empty()) {
            if (catalog.isSingleMultiplicityInDirection(relLabel, FWD)) {
                initPropertyColumnsForRelLabel(
                    catalog, numNodesPerLabel, directory, bufferManager, relLabel, FWD);
            } else if (catalog.isSingleMultiplicityInDirection(relLabel, BWD)) {
                initPropertyColumnsForRelLabel(
                    catalog, numNodesPerLabel, directory, bufferManager, relLabel, BWD);
            } else {
                initPropertyListsForRelLabel(
                    catalog, numNodesPerLabel, directory, bufferManager, relLabel);
            }
        }
    }
    logger->info("Initializing PropertyLists and PropertyColumns Done.");
}

void RelsStore::initPropertyColumnsForRelLabel(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    const label_t& relLabel, const Direction& dir) {
    logger->debug("Initializing PropertyColumns: relLabel {}", relLabel);
    for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDirection(relLabel, dir)) {
        auto& properties = catalog.getRelProperties(relLabel);
        propertyColumns[nodeLabel][relLabel].resize(properties.size());
        for (auto& property : properties) {
            auto fname = getRelPropertyColumnFName(directory, relLabel, nodeLabel, property.name);
            logger->debug("DIR {} nodeLabel {} propertyIdx {} type {} name `{}`", dir, nodeLabel,
                property.id, property.dataType, property.name);
            switch (property.dataType) {
            case INT64:
                propertyColumns[nodeLabel][relLabel][property.id] =
                    make_unique<PropertyColumnInt64>(
                        fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case DOUBLE:
                propertyColumns[nodeLabel][relLabel][property.id] =
                    make_unique<PropertyColumnDouble>(
                        fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case BOOL:
                propertyColumns[nodeLabel][relLabel][property.id] = make_unique<PropertyColumnBool>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case STRING:
                propertyColumns[nodeLabel][relLabel][property.id] =
                    make_unique<PropertyColumnString>(
                        fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            default:
                throw invalid_argument("Invalid type for property column creation.");
            }
        }
    }
    logger->debug("Initializing PropertyColumns done.");
}

void RelsStore::initPropertyListsForRelLabel(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    const label_t& relLabel) {
    logger->debug("Initializing PropertyLists: relLabel {}", relLabel);
    for (auto& dir : DIRECTIONS) {
        for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDirection(relLabel, dir)) {
            auto& properties = catalog.getRelProperties(relLabel);
            propertyLists[dir][nodeLabel][relLabel].resize(properties.size());
            auto adjListsHeaders = adjLists[dir][nodeLabel][relLabel]->getHeaders();
            for (auto& property : properties) {
                auto fname =
                    getRelPropertyListsFName(directory, relLabel, nodeLabel, dir, property.name);
                logger->debug("DIR {} nodeLabel {} propertyIdx {} type {} name `{}`", dir,
                    nodeLabel, property.id, property.dataType, property.name);
                switch (property.dataType) {
                case INT64:
                    propertyLists[dir][nodeLabel][relLabel][property.id] =
                        make_unique<RelPropertyListsInt64>(fname, adjListsHeaders, bufferManager);
                    break;
                case DOUBLE:
                    propertyLists[dir][nodeLabel][relLabel][property.id] =
                        make_unique<RelPropertyListsDouble>(fname, adjListsHeaders, bufferManager);
                    break;
                case BOOL:
                    propertyLists[dir][nodeLabel][relLabel][property.id] =
                        make_unique<RelPropertyListsBool>(fname, adjListsHeaders, bufferManager);
                    break;
                case STRING:
                    propertyLists[dir][nodeLabel][relLabel][property.id] =
                        make_unique<RelPropertyListsString>(fname, adjListsHeaders, bufferManager);
                    break;
                case DATE:
                    propertyLists[dir][nodeLabel][relLabel][property.id] =
                        make_unique<RelPropertyListsDate>(fname, adjListsHeaders, bufferManager);
                    break;
                default:
                    throw invalid_argument("Invalid type for property list creation.");
                }
            }
        }
    }
    logger->debug("Initializing PropertyLists done.");
}

} // namespace storage
} // namespace graphflow
