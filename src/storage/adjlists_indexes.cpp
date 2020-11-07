#include "src/storage/include/adjlists_indexes.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

AdjListsIndexes::AdjListsIndexes(Catalog &catalog, vector<uint64_t> &numNodesPerLabel,
    const string &directory, BufferManager &bufferManager) {
    initSingleCardinalityAdjListsIndexes(catalog, numNodesPerLabel, directory, bufferManager);
}

void AdjListsIndexes::initSingleCardinalityAdjListsIndexes(Catalog &catalog,
    vector<uint64_t> &numNodesPerLabel, const string &directory, BufferManager &bufferManager) {
    singleCardinalityAdjLists.resize(2);
    for (auto direction : DIRECTIONS) {
        for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
            auto &relLabels = catalog.getRelLabelsForNodeLabelDirection(nodeLabel, direction);
            singleCardinalityAdjLists[direction][nodeLabel].resize(relLabels.size());
            for (auto relLabel : relLabels) {
                if (catalog.isSingleCaridinalityInDir(relLabel, direction)) {
                    auto &nbrNodeLabels = catalog.getNodeLabelsForRelLabelDir(relLabel, !direction);
                    singleCardinalityAdjLists[direction][nodeLabel][relLabel] =
                        initSingleCardinalityAdjListsIndex(nbrNodeLabels, numNodesPerLabel,
                            catalog.getNodeLabelsCount(), directory, nodeLabel, relLabel, direction,
                            bufferManager);
                }
            }
        }
    }
}

unique_ptr<ColumnBase> AdjListsIndexes::initSingleCardinalityAdjListsIndex(
    const vector<gfLabel_t> &nbrNodeLabels, const vector<uint64_t> &numNodesPerLabel,
    uint32_t numNodeLabels, const string &directory, gfLabel_t nodeLabel, gfLabel_t relLabel,
    Direction direction, BufferManager &bufferManager) {
    auto scheme = getColumnAdjListsIndexScheme(nbrNodeLabels, numNodesPerLabel, numNodeLabels);
    auto fname = getColumnAdjListIndexFname(directory, nodeLabel, relLabel, direction);
    auto numElements = numNodesPerLabel[nodeLabel];
    switch (scheme.first) {
    case 0:
        switch (scheme.second) {
        case 1:
            return make_unique<Column1BOffset>("", fname, numElements, bufferManager);
        case 2:
            return make_unique<Column2BOffset>("", fname, numElements, bufferManager);
        case 4:
            return make_unique<Column4BOffset>("", fname, numElements, bufferManager);
        case 8:
            return make_unique<Column8BOffset>("", fname, numElements, bufferManager);
        }
    case 1:
        switch (scheme.second) {
        case 1:
            return make_unique<Column1BLabel1BOffset>(fname, numElements, bufferManager);
        case 2:
            return make_unique<Column1BLabel2BOffset>(fname, numElements, bufferManager);
        case 4:
            return make_unique<Column1BLabel4BOffset>(fname, numElements, bufferManager);
        case 8:
            return make_unique<Column1BLabel8BOffset>(fname, numElements, bufferManager);
        }
    case 2:
        switch (scheme.second) {
        case 1:
            return make_unique<Column2BLabel1BOffset>(fname, numElements, bufferManager);
        case 2:
            return make_unique<Column2BLabel2BOffset>(fname, numElements, bufferManager);
        case 4:
            return make_unique<Column2BLabel4BOffset>(fname, numElements, bufferManager);
        case 8:
            return make_unique<Column2BLabel8BOffset>(fname, numElements, bufferManager);
        }
    case 4:
        switch (scheme.second) {
        case 1:
            return make_unique<Column4BLabel1BOffset>(fname, numElements, bufferManager);
        case 2:
            return make_unique<Column4BLabel2BOffset>(fname, numElements, bufferManager);
        case 4:
            return make_unique<Column4BLabel4BOffset>(fname, numElements, bufferManager);
        case 8:
            return make_unique<Column4BLabel8BOffset>(fname, numElements, bufferManager);
        }
    }
    throw invalid_argument(
        "Unhandled case of Column Adjacency Lists Index creation. numLabelBytes: " +
        to_string(scheme.first) + ", numOffsetBytes: " + to_string(scheme.second));
}

pair<uint32_t, uint32_t> AdjListsIndexes::getColumnAdjListsIndexScheme(
    const vector<gfLabel_t> &nbrNodeLabels, const vector<uint64_t> &numNodesPerLabel,
    uint32_t numNodeLabels) {
    auto maxNodeOffsetToFit = 0ull;
    for (auto nodeLabel : nbrNodeLabels) {
        if (numNodesPerLabel[nodeLabel] > maxNodeOffsetToFit) {
            maxNodeOffsetToFit = numNodesPerLabel[nodeLabel];
        }
    }
    auto maxLabelToFit = 1 == nbrNodeLabels.size() ? 0 : numNodeLabels - 1;
    auto numLabelBytes = maxLabelToFit == 0 ? 0 : getNumBytesForEncoding(maxLabelToFit);
    auto numOffsetBytes = getNumBytesForEncoding(maxNodeOffsetToFit);
    return make_pair(numLabelBytes, numOffsetBytes);
}

uint32_t AdjListsIndexes::getNumBytesForEncoding(uint64_t val) {
    auto numBytes = 1;
    while (val > (1ull << (8 * numBytes)) - 2) {
        numBytes <<= 1;
    }
    return numBytes;
}

} // namespace storage
} // namespace graphflow
