#include "src/storage/include/stores/rels_store.h"

#include "src/storage/include/structures/property_column.h"
#include "src/storage/include/structures/property_lists.h"

namespace graphflow {
namespace storage {

RelsStore::RelsStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    initPropertyListsAndColumns(catalog, numNodesPerLabel, directory, bufferManager);
    initAdjListsAndColumns(catalog, numNodesPerLabel, directory, bufferManager);
}

pair<uint32_t, uint32_t> RelsStore::getNumBytesScheme(const vector<label_t>& nbrNodeLabels,
    const vector<uint64_t>& numNodesPerLabel, const uint32_t& numNodeLabels) {
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

uint32_t RelsStore::getNumBytesForEncoding(const uint64_t& val, const uint8_t& minNumBytes) {
    auto numBytes = minNumBytes;
    while (val > (1ull << (8 * numBytes)) - 2) {
        numBytes <<= 1;
    }
    return numBytes;
}

void RelsStore::initPropertyListsAndColumns(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory,
    BufferManager& bufferManager) {
    propertyColumns.resize(catalog.getNodeLabelsCount());
    propertyLists[FWD].resize(catalog.getNodeLabelsCount());
    propertyLists[BWD].resize(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        propertyColumns[nodeLabel].resize(catalog.getRelLabelsCount());
        propertyLists[FWD][nodeLabel].resize(catalog.getRelLabelsCount());
        propertyLists[BWD][nodeLabel].resize(catalog.getRelLabelsCount());
    }
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

void RelsStore::initPropertyColumnsForRelLabel(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    const label_t& relLabel, const Direction& dir) {
    propertyColumns[relLabel].resize(catalog.getNodeLabelsCount());
    for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDir(relLabel, dir)) {
        auto& propertyMap = catalog.getPropertyMapForRelLabel(relLabel);
        propertyColumns[relLabel][nodeLabel].resize(propertyMap.size());
        for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
            auto idx = property->second.idx;
            auto fname = getRelPropertyColumnFname(directory, relLabel, nodeLabel, property->first);
            switch (property->second.dataType) {
            case INT:
                propertyColumns[relLabel][nodeLabel][idx] = make_unique<PropertyColumnInt>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case DOUBLE:
                propertyColumns[relLabel][nodeLabel][idx] = make_unique<PropertyColumnDouble>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case BOOL:
                propertyColumns[relLabel][nodeLabel][idx] = make_unique<PropertyColumnBool>(
                    fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case STRING:
                throw invalid_argument("not supported.");
                propertyColumns[relLabel][nodeLabel][idx] = nullptr;
            }
        }
    }
}

void RelsStore::initPropertyListsForRelLabel(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    const label_t& relLabel) {
    for (auto& dir : DIRS) {
        propertyLists[dir][relLabel].resize(catalog.getNodeLabelsCount());
        for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDir(relLabel, dir)) {
            auto& propertyMap = catalog.getPropertyMapForRelLabel(relLabel);
            propertyLists[dir][relLabel][nodeLabel].resize(propertyMap.size());
            for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
                auto fname =
                    getRelPropertyListsFname(directory, relLabel, nodeLabel, dir, property->first);
                auto idx = property->second.idx;
                switch (property->second.dataType) {
                case INT:
                    propertyLists[dir][relLabel][nodeLabel][idx] =
                        make_unique<RelPropertyListsInt>(fname, bufferManager);
                    break;
                case DOUBLE:
                    propertyLists[dir][relLabel][nodeLabel][idx] =
                        make_unique<RelPropertyListsDouble>(fname, bufferManager);
                    break;
                case BOOL:
                    propertyLists[dir][relLabel][nodeLabel][idx] =
                        make_unique<RelPropertyListsBool>(fname, bufferManager);
                    break;
                case STRING:
                    throw invalid_argument("not supported.");
                    propertyLists[dir][relLabel][nodeLabel][idx] = nullptr;
                }
            }
        }
    }
}

void RelsStore::initAdjListsAndColumns(const Catalog& catalog,
    const vector<uint64_t>& numNodesPerLabel, const string& directory,
    BufferManager& bufferManager) {
    for (auto direction : DIRS) {
        adjColumns[direction].resize(catalog.getNodeLabelsCount());
        adjLists[direction].resize(catalog.getNodeLabelsCount());
        for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
            adjColumns[direction][nodeLabel].resize(catalog.getRelLabelsCount());
            adjLists[direction][nodeLabel].resize(catalog.getRelLabelsCount());
            for (auto relLabel : catalog.getRelLabelsForNodeLabelDirection(nodeLabel, direction)) {
                auto numBytesScheme =
                    getNumBytesScheme(catalog.getNodeLabelsForRelLabelDir(relLabel, !direction),
                        numNodesPerLabel, catalog.getNodeLabelsCount());
                if (catalog.isSingleCaridinalityInDir(relLabel, direction)) {
                    auto fname = getAdjColumnIndexFname(directory, nodeLabel, relLabel, direction);
                    adjColumns[direction][nodeLabel][relLabel] =
                        make_unique<AdjColumn>(fname, numNodesPerLabel[nodeLabel],
                            numBytesScheme.first, numBytesScheme.second, bufferManager);
                } else {
                    auto fname = getAdjListsIndexFname(directory, nodeLabel, relLabel, direction);
                    adjLists[direction][nodeLabel][relLabel] = make_unique<AdjLists>(
                        fname, numBytesScheme.first, numBytesScheme.second, bufferManager);
                }
            }
        }
    }
}

} // namespace storage
} // namespace graphflow
