#include "src/storage/include/store/rels_store.h"

#include "src/common/include/compression_scheme.h"

namespace graphflow {
namespace storage {

RelsStore::RelsStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager)
    : logger{spdlog::get("storage")} {
    initAdjColumns(catalog, numNodesPerLabel, directory, bufferManager);
    initAdjLists(catalog, numNodesPerLabel, directory, bufferManager);
    initPropertyListsAndColumns(catalog, numNodesPerLabel, directory, bufferManager);
}

void RelsStore::initAdjColumns(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    logger->info("Initializing AdjColumns.");
    for (auto dir : DIRS) {
        adjColumns[dir].resize(catalog.getNodeLabelsCount());
        for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
            adjColumns[dir][nodeLabel].resize(catalog.getRelLabelsCount());
            for (auto relLabel : catalog.getRelLabelsForNodeLabelDirection(nodeLabel, dir)) {
                if (catalog.isSingleCaridinalityInDir(relLabel, dir)) {
                    NodeIDCompressionScheme nodeIDCompressionScheme{
                        catalog.getNodeLabelsForRelLabelDir(relLabel, !dir), numNodesPerLabel,
                        catalog.getNodeLabelsCount()};
                    logger->debug("DIR {} nodeLabel {} relLabel {} compressionScheme {},{}", dir,
                        nodeLabel, relLabel, nodeIDCompressionScheme.getNumBytesForLabel(),
                        nodeIDCompressionScheme.getNumBytesForOffset());
                    auto fname = getAdjColumnFname(directory, relLabel, nodeLabel, dir);
                    adjColumns[dir][nodeLabel][relLabel] = make_unique<AdjColumn>(
                        fname, numNodesPerLabel[nodeLabel], bufferManager, nodeIDCompressionScheme);
                }
            }
        }
    }
    logger->info("Done.");
}

void RelsStore::initAdjLists(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    logger->info("Initializing AdjLists.");
    for (auto dir : DIRS) {
        adjLists[dir].resize(catalog.getNodeLabelsCount());
        for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
            adjLists[dir][nodeLabel].resize(catalog.getRelLabelsCount());
            for (auto relLabel : catalog.getRelLabelsForNodeLabelDirection(nodeLabel, dir)) {
                if (!catalog.isSingleCaridinalityInDir(relLabel, dir)) {
                    NodeIDCompressionScheme nodeIDCompressionScheme{
                        catalog.getNodeLabelsForRelLabelDir(relLabel, !dir), numNodesPerLabel,
                        catalog.getNodeLabelsCount()};
                    logger->debug("DIR {} nodeLabel {} relLabel {} compressionScheme {},{}", dir,
                        nodeLabel, relLabel, nodeIDCompressionScheme.getNumBytesForLabel(),
                        nodeIDCompressionScheme.getNumBytesForOffset());
                    auto fname = getAdjListsFname(directory, relLabel, nodeLabel, dir);
                    adjLists[dir][nodeLabel][relLabel] =
                        make_unique<AdjLists>(fname, bufferManager, nodeIDCompressionScheme);
                }
            }
        }
    }
    logger->info("Done.");
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
        if (0 != catalog.getPropertyKeyMapForRelLabel(relLabel).size()) {
            if (catalog.isSingleCaridinalityInDir(relLabel, FWD)) {
                initPropertyColumnsForRelLabel(
                    catalog, numNodesPerLabel, directory, bufferManager, relLabel, FWD);
            } else if (catalog.isSingleCaridinalityInDir(relLabel, BWD)) {
                initPropertyColumnsForRelLabel(
                    catalog, numNodesPerLabel, directory, bufferManager, relLabel, BWD);
            } else {
                initPropertyListsForRelLabel(
                    catalog, numNodesPerLabel, directory, bufferManager, relLabel);
            }
        }
    }
    logger->info("Done.");
}

void RelsStore::initPropertyColumnsForRelLabel(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    const label_t& relLabel, const Direction& dir) {
    logger->debug("Initializing PropertyColumns: relLabel {}", relLabel);
    for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDir(relLabel, dir)) {
        auto& propertyMap = catalog.getPropertyKeyMapForRelLabel(relLabel);
        propertyColumns[nodeLabel][relLabel].resize(propertyMap.size());
        for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
            auto idx = property->second.idx;
            auto fname = getRelPropertyColumnFname(directory, relLabel, nodeLabel, property->first);
            logger->debug("DIR {} nodeLabel {} propertyIdx {} type {} name `{}`", dir, nodeLabel,
                idx, property->second.dataType, property->first);
            switch (property->second.dataType) {
            case INT32:
                propertyColumns[nodeLabel][relLabel][idx] = make_unique<PropertyColumnInt>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case DOUBLE:
                propertyColumns[nodeLabel][relLabel][idx] = make_unique<PropertyColumnDouble>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case BOOL:
                propertyColumns[nodeLabel][relLabel][idx] = make_unique<PropertyColumnBool>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case STRING:
                propertyColumns[nodeLabel][relLabel][idx] = make_unique<PropertyColumnString>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            default:
                throw invalid_argument("invalid type for property list creation.");
            }
        }
    }
    logger->debug("Done.");
}

void RelsStore::initPropertyListsForRelLabel(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    const label_t& relLabel) {
    logger->debug("Initializing PropertyLists: relLabel {}", relLabel);
    for (auto& dir : DIRS) {
        for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDir(relLabel, dir)) {
            auto& propertyMap = catalog.getPropertyKeyMapForRelLabel(relLabel);
            propertyLists[dir][nodeLabel][relLabel].resize(propertyMap.size());
            auto adjListsHeaders = adjLists[dir][nodeLabel][relLabel]->getHeaders();
            for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
                auto fname =
                    getRelPropertyListsFname(directory, relLabel, nodeLabel, dir, property->first);
                auto idx = property->second.idx;
                logger->debug("DIR {} nodeLabel {} propertyIdx {} type {} name `{}`", dir,
                    nodeLabel, idx, property->second.dataType, property->first);
                switch (property->second.dataType) {
                case INT32:
                    propertyLists[dir][nodeLabel][relLabel][idx] =
                        make_unique<RelPropertyListsInt>(fname, adjListsHeaders, bufferManager);
                    break;
                case DOUBLE:
                    propertyLists[dir][nodeLabel][relLabel][idx] =
                        make_unique<RelPropertyListsDouble>(fname, adjListsHeaders, bufferManager);
                    break;
                case BOOL:
                    propertyLists[dir][nodeLabel][relLabel][idx] =
                        make_unique<RelPropertyListsBool>(fname, adjListsHeaders, bufferManager);
                    break;
                case STRING:
                    propertyLists[dir][nodeLabel][relLabel][idx] =
                        make_unique<RelPropertyListsString>(fname, adjListsHeaders, bufferManager);
                default:
                    throw invalid_argument("invalid type for property list creation.");
                }
            }
        }
    }
    logger->debug("Done.");
}

} // namespace storage
} // namespace graphflow
