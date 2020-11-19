#include "src/storage/include/indexes.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

Indexes::Indexes(Catalog &catalog, vector<uint64_t> &numNodesPerLabel, const string &directory,
    BufferManager &bufferManager) {
    initAdjEdgesIndexes(catalog, numNodesPerLabel, directory, bufferManager);
}

void Indexes::initAdjEdgesIndexes(Catalog &catalog, vector<uint64_t> &numNodesPerLabel,
    const string &directory, BufferManager &bufferManager) {
    adjEdgesIndexes.resize(2);
    for (auto direction : DIRECTIONS) {
        for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
            auto &relLabels = catalog.getRelLabelsForNodeLabelDirection(nodeLabel, direction);
            adjEdgesIndexes[direction][nodeLabel].resize(relLabels.size());
            for (auto relLabel : relLabels) {
                if (catalog.isSingleCaridinalityInDir(relLabel, direction)) {
                    auto &nbrNodeLabels = catalog.getNodeLabelsForRelLabelDir(relLabel, !direction);
                    adjEdgesIndexes[direction][nodeLabel][relLabel] = initAdjEdgesIndex(
                        nbrNodeLabels, numNodesPerLabel, catalog.getNodeLabelsCount(), directory,
                        nodeLabel, relLabel, direction, bufferManager);
                }
            }
        }
    }
}

unique_ptr<ColumnBase> Indexes::initAdjEdgesIndex(const vector<gfLabel_t> &nbrNodeLabels,
    const vector<uint64_t> &numNodesPerLabel, uint32_t numNodeLabels, const string &directory,
    gfLabel_t nodeLabel, gfLabel_t relLabel, Direction direction, BufferManager &bufferManager) {
    auto scheme = getNumBytesScheme(nbrNodeLabels, numNodesPerLabel, numNodeLabels);
    auto fname = getAdjEdgesIndexFname(directory, nodeLabel, relLabel, direction);
    auto numElements = numNodesPerLabel[nodeLabel];
    switch (scheme.first) {
    case 0:
        switch (scheme.second) {
        case 2:
            return make_unique<Column2BOffset>("", fname, numElements, bufferManager);
        case 4:
            return make_unique<Column4BOffset>("", fname, numElements, bufferManager);
        case 8:
            return make_unique<Column8BOffset>("", fname, numElements, bufferManager);
        }
    case 1:
        switch (scheme.second) {
        case 2:
            return make_unique<Column1BLabel2BOffset>(fname, numElements, bufferManager);
        case 4:
            return make_unique<Column1BLabel4BOffset>(fname, numElements, bufferManager);
        case 8:
            return make_unique<Column1BLabel8BOffset>(fname, numElements, bufferManager);
        }
    case 2:
        switch (scheme.second) {
        case 2:
            return make_unique<Column2BLabel2BOffset>(fname, numElements, bufferManager);
        case 4:
            return make_unique<Column2BLabel4BOffset>(fname, numElements, bufferManager);
        case 8:
            return make_unique<Column2BLabel8BOffset>(fname, numElements, bufferManager);
        }
    case 4:
        switch (scheme.second) {
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

pair<uint32_t, uint32_t> Indexes::getNumBytesScheme(const vector<gfLabel_t> &nbrNodeLabels,
    const vector<uint64_t> &numNodesPerLabel, uint32_t numNodeLabels) {
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

uint32_t Indexes::getNumBytesForEncoding(uint64_t val, uint8_t minNumBytes) {
    auto numBytes = minNumBytes;
    while (val > (1ull << (8 * numBytes)) - 2) {
        numBytes <<= 1;
    }
    return numBytes;
}

} // namespace storage
} // namespace graphflow
