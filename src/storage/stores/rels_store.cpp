#include "src/storage/include/stores/rels_store.h"

#include "src/storage/include/structures/property_lists.h"

namespace graphflow {
namespace storage {

RelsStore::RelsStore(Catalog& catalog, vector<uint64_t>& numNodesPerLabel, const string& directory,
    BufferManager& bufferManager) {
    propertyColumns.resize(catalog.getRelLabelsCount());
    propertyLists[FWD].resize(catalog.getRelLabelsCount());
    propertyLists[BWD].resize(catalog.getRelLabelsCount());
    for (auto relLabel = 0u; catalog.getRelLabelsCount(); relLabel++) {
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

pair<uint32_t, uint32_t> RelsStore::getNumBytesScheme(const vector<gfLabel_t>& nbrNodeLabels,
    const vector<uint64_t>& numNodesPerLabel, uint32_t numNodeLabels) {
    auto maxNodeOffsetToFit = 0ull;
    for (auto nodeLabel : nbrNodeLabels) {
        if (numNodesPerLabel[nodeLabel] > maxNodeOffsetToFit) {
            maxNodeOffsetToFit = numNodesPerLabel[nodeLabel];
        }
    }
    auto maxLabelToFit = 1 == nbrNodeLabels.size() ? 0 : numNodeLabels - 1;
    auto numBytesPerlabel =
        maxLabelToFit == 0 ? 0 : getNumBytesForEncoding(maxLabelToFit, 1 /*min num bytes*/);
    auto numBytesPerOffset = getNumBytesForEncoding(maxNodeOffsetToFit, 2 /*min num bytes*/);
    return make_pair(numBytesPerlabel, numBytesPerOffset);
}

uint32_t RelsStore::getNumBytesForEncoding(uint64_t val, uint8_t minNumBytes) {
    auto numBytes = minNumBytes;
    while (val > (1ull << (8 * numBytes)) - 2) {
        numBytes <<= 1;
    }
    return numBytes;
}

void RelsStore::initPropertyColumnsForRelLabel(Catalog& catalog, vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager, gfLabel_t relLabel, Direction dir) {
    propertyColumns[relLabel].resize(catalog.getNodeLabelsCount());
    for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDir(relLabel, dir)) {
        auto& propertyMap = catalog.getPropertyMapForRelLabel(relLabel);
        propertyColumns[relLabel][nodeLabel].resize(propertyMap.size());
        for (auto i = 0u; i < propertyMap.size(); i++) {
            auto& property = propertyMap[i];
            auto fname =
                getRelPropertyColumnFname(directory, relLabel, nodeLabel, property.propertyName);
            switch (property.dataType) {
            case INT:
                propertyColumns[relLabel][nodeLabel][i] = make_unique<PropertyColumnInteger>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case DOUBLE:
                propertyColumns[relLabel][nodeLabel][i] = make_unique<PropertyColumnDouble>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case BOOLEAN:
                propertyColumns[relLabel][nodeLabel][i] = make_unique<PropertyColumnBoolean>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            default:
                throw std::invalid_argument("not supported.");
                propertyColumns[relLabel][nodeLabel][i] = nullptr;
            }
        }
    }
}

void RelsStore::initPropertyListsForRelLabel(Catalog& catalog, vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager, gfLabel_t relLabel) {
    for (auto& dir : DIRS) {
        propertyLists[dir][relLabel].resize(catalog.getNodeLabelsCount());
        for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDir(relLabel, dir)) {
            auto& propertyMap = catalog.getPropertyMapForRelLabel(relLabel);
            propertyLists[dir][relLabel][nodeLabel].resize(propertyMap.size());
            for (auto i = 0u; i < propertyMap.size(); i++) {
                auto& property = propertyMap[i];
                auto fname = getRelPropertyListsFname(
                    directory, relLabel, nodeLabel, dir, property.propertyName);
                switch (property.dataType) {
                case INT:
                    propertyLists[dir][relLabel][nodeLabel][i] =
                        make_unique<RelPropertyListsInteger>(fname, bufferManager);
                    break;
                case DOUBLE:
                    propertyLists[dir][relLabel][nodeLabel][i] =
                        make_unique<RelPropertyListsDouble>(fname, bufferManager);
                    break;
                case BOOLEAN:
                    propertyLists[dir][relLabel][nodeLabel][i] =
                        make_unique<RelPropertyListsBoolean>(fname, bufferManager);
                    break;
                default:
                    throw std::invalid_argument("not supported.");
                    propertyLists[dir][relLabel][nodeLabel][i] = nullptr;
                }
            }
        }
    }
}

void RelsStore::initPropertyListsForRelLabel(Catalog& catalog, vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    for (auto direction : DIRS) {
        for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
            auto& relLabels = catalog.getRelLabelsForNodeLabelDirection(nodeLabel, direction);
            adjEdgesIndexes[direction][nodeLabel].resize(relLabels.size());
            for (auto relLabel : relLabels) {
                auto numBytesScheme =
                    getNumBytesScheme(catalog.getNodeLabelsForRelLabelDir(relLabel, !direction),
                        numNodesPerLabel, catalog.getNodeLabelsCount());
                if (catalog.isSingleCaridinalityInDir(relLabel, direction)) {
                    auto fname = getAdjRelsIndexFname(directory, nodeLabel, relLabel, direction);
                    adjEdgesIndexes[direction][nodeLabel][relLabel] =
                        make_unique<AdjRels>(fname, numNodesPerLabel[nodeLabel],
                            numBytesScheme.first, numBytesScheme.second, bufferManager);
                } else {
                    auto fname = getAdjListsIndexFname(directory, nodeLabel, relLabel, direction);
                    adjListsIndexes[direction][nodeLabel][relLabel] = make_unique<AdjLists>(
                        fname, numBytesScheme.first, numBytesScheme.second, bufferManager);
                }
            }
        }
    }
}

} // namespace storage
} // namespace graphflow
