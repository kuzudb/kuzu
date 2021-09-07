#include "src/storage/include/store/rels_store.h"

namespace graphflow {
namespace storage {

RelsStore::RelsStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager, bool isInMemoryMode)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")} {
    initAdjColumns(catalog, numNodesPerLabel, directory, bufferManager, isInMemoryMode);
    initAdjLists(catalog, numNodesPerLabel, directory, bufferManager, isInMemoryMode);
    initPropertyListsAndColumns(
        catalog, numNodesPerLabel, directory, bufferManager, isInMemoryMode);
}

void RelsStore::initAdjColumns(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager, bool isInMemoryMode) {
    logger->info("Initializing AdjColumns.");
    for (auto direction : DIRECTIONS) {
        auto numNodeLabels = catalog.getNodeLabelsCount();
        auto numRelLabels = catalog.getRelLabelsCount();
        adjColumns[direction].resize(numNodeLabels);
        for (auto nodeLabel = 0u; nodeLabel < numNodeLabels; nodeLabel++) {
            adjColumns[direction][nodeLabel].resize(numRelLabels);
            for (auto relLabel : catalog.getRelLabelsForNodeLabelDirection(nodeLabel, direction)) {
                if (catalog.isSingleMultiplicityInDirection(relLabel, direction)) {
                    const auto& nodeLabels =
                        catalog.getNodeLabelsForRelLabelDirection(relLabel, !direction);
                    NodeIDCompressionScheme nodeIDCompressionScheme(
                        nodeLabels, numNodesPerLabel, numNodeLabels);
                    if (nodeLabels.size() == 1) {
                        nodeIDCompressionScheme.setCommonLabel(*nodeLabels.begin());
                    }
                    logger->debug(
                        "DIRECTION {} nodeLabel {} relLabel {} compressionScheme {},{},{}",
                        direction, nodeLabel, relLabel,
                        nodeIDCompressionScheme.getNumBytesForLabel(),
                        nodeIDCompressionScheme.getNumBytesForOffset(),
                        nodeIDCompressionScheme.getCommonLabel());
                    auto fName = getAdjColumnFName(directory, relLabel, nodeLabel, direction);
                    adjColumns[direction][nodeLabel][relLabel] =
                        make_unique<AdjColumn>(fName, numNodesPerLabel[nodeLabel], bufferManager,
                            nodeIDCompressionScheme, isInMemoryMode);
                }
            }
        }
    }
    logger->info("Initializing AdjColumns done.");
}

void RelsStore::initAdjLists(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager, bool isInMemoryMode) {
    logger->info("Initializing AdjLists.");
    for (auto direction : DIRECTIONS) {
        adjLists[direction].resize(catalog.getNodeLabelsCount());
        for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
            adjLists[direction][nodeLabel].resize(catalog.getRelLabelsCount());
            for (auto relLabel : catalog.getRelLabelsForNodeLabelDirection(nodeLabel, direction)) {
                if (!catalog.isSingleMultiplicityInDirection(relLabel, direction)) {
                    const auto& nodeLabels =
                        catalog.getNodeLabelsForRelLabelDirection(relLabel, !direction);
                    NodeIDCompressionScheme nodeIDCompressionScheme(
                        nodeLabels, numNodesPerLabel, catalog.getNodeLabelsCount());
                    if (nodeLabels.size() == 1) {
                        nodeIDCompressionScheme.setCommonLabel(*nodeLabels.begin());
                    };
                    logger->debug(
                        "DIRECTION {} nodeLabel {} relLabel {} compressionScheme {},{},{}",
                        direction, nodeLabel, relLabel,
                        nodeIDCompressionScheme.getNumBytesForLabel(),
                        nodeIDCompressionScheme.getNumBytesForOffset(),
                        nodeIDCompressionScheme.getCommonLabel());
                    auto fName = getAdjListsFName(directory, relLabel, nodeLabel, direction);
                    adjLists[direction][nodeLabel][relLabel] = make_unique<AdjLists>(
                        fName, bufferManager, nodeIDCompressionScheme, isInMemoryMode);
                }
            }
        }
    }
    logger->info("Initializing AdjLists done.");
}

void RelsStore::initPropertyListsAndColumns(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    bool isInMemoryMode) {
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
                initPropertyColumnsForRelLabel(catalog, numNodesPerLabel, directory, bufferManager,
                    relLabel, FWD, isInMemoryMode);
            } else if (catalog.isSingleMultiplicityInDirection(relLabel, BWD)) {
                initPropertyColumnsForRelLabel(catalog, numNodesPerLabel, directory, bufferManager,
                    relLabel, BWD, isInMemoryMode);
            } else {
                initPropertyListsForRelLabel(
                    catalog, numNodesPerLabel, directory, bufferManager, relLabel, isInMemoryMode);
            }
        }
    }
    logger->info("Initializing PropertyLists and PropertyColumns Done.");
}

void RelsStore::initPropertyColumnsForRelLabel(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    const label_t& relLabel, const Direction& dir, bool isInMemoryMode) {
    logger->debug("Initializing PropertyColumns: relLabel {}", relLabel);
    for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDirection(relLabel, dir)) {
        auto& properties = catalog.getRelProperties(relLabel);
        propertyColumns[nodeLabel][relLabel].resize(properties.size());
        for (auto& property : properties) {
            auto fName = getRelPropertyColumnFName(directory, relLabel, nodeLabel, property.name);
            logger->debug("DIR {} nodeLabel {} propertyIdx {} type {} name `{}`", dir, nodeLabel,
                property.id, property.dataType, property.name);
            propertyColumns[nodeLabel][relLabel][property.id] = ColumnFactory::getColumn(fName,
                property.dataType, numNodesPerLabel[nodeLabel], bufferManager, isInMemoryMode);
        }
    }
    logger->debug("Initializing PropertyColumns done.");
}

void RelsStore::initPropertyListsForRelLabel(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    const label_t& relLabel, bool isInMemoryMode) {
    logger->debug("Initializing PropertyLists: relLabel {}", relLabel);
    for (auto& dir : DIRECTIONS) {
        for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDirection(relLabel, dir)) {
            auto& properties = catalog.getRelProperties(relLabel);
            propertyLists[dir][nodeLabel][relLabel].resize(properties.size());
            auto adjListsHeaders = adjLists[dir][nodeLabel][relLabel]->getHeaders();
            for (auto& property : properties) {
                auto fName =
                    getRelPropertyListsFName(directory, relLabel, nodeLabel, dir, property.name);
                logger->debug("DIR {} nodeLabel {} propertyIdx {} type {} name `{}`", dir,
                    nodeLabel, property.id, property.dataType, property.name);
                propertyLists[dir][nodeLabel][relLabel][property.id] = ListsFactory::getLists(
                    fName, property.dataType, adjListsHeaders, bufferManager, isInMemoryMode);
            }
        }
    }
    logger->debug("Initializing PropertyLists done.");
}

} // namespace storage
} // namespace graphflow
