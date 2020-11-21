#include "src/storage/include/indexes.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

Indexes::Indexes(Catalog &catalog, vector<uint64_t> &numNodesPerLabel, const string &directory,
    BufferManager &bufferManager) {
    for (auto direction : DIRECTIONS) {
        for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
            auto &relLabels = catalog.getRelLabelsForNodeLabelDirection(nodeLabel, direction);
            adjEdgesIndexes[direction][nodeLabel].resize(relLabels.size());
            for (auto relLabel : relLabels) {
                auto numBytesScheme =
                    getNumBytesScheme(catalog.getNodeLabelsForRelLabelDir(relLabel, !direction),
                        numNodesPerLabel, catalog.getNodeLabelsCount());
                if (catalog.isSingleCaridinalityInDir(relLabel, direction)) {
                    auto fname =
                        AdjEdges::getAdjEdgesIndexFname(directory, nodeLabel, relLabel, direction);
                    adjEdgesIndexes[direction][nodeLabel][relLabel] =
                        make_unique<AdjEdges>(fname, numNodesPerLabel[nodeLabel],
                            numBytesScheme.first, numBytesScheme.second, bufferManager);
                } else {
                    auto fname =
                        AdjLists::getAdjListsIndexFname(directory, nodeLabel, relLabel, direction);
                    adjListsIndexes[direction][nodeLabel][relLabel] = make_unique<AdjLists>(
                        fname, numBytesScheme.first, numBytesScheme.second, bufferManager);
                }
            }
        }
    }
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
