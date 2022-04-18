#include "src/storage/include/compression_scheme.h"

#include <stdexcept>

namespace graphflow {
namespace common {

NodeIDCompressionScheme::NodeIDCompressionScheme(const unordered_set<label_t>& nbrNodeLabels,
    const vector<uint64_t>& numNodesPerLabel, uint32_t numNodeLabels)
    : commonLabel(0) {
    uint64_t maxNodeOffsetToFit = 0;
    for (auto nodeLabel : nbrNodeLabels) {
        if (numNodesPerLabel[nodeLabel] > maxNodeOffsetToFit) {
            maxNodeOffsetToFit = numNodesPerLabel[nodeLabel];
        }
    }
    if (nbrNodeLabels.size() == 1) {
        numBytesForLabel = 0;
        commonLabel = *nbrNodeLabels.begin();
    } else {
        numBytesForLabel = getNumBytesForEncoding(numNodeLabels - 1, 1 /* min num bytes */);
    }
    numBytesForOffset = getNumBytesForEncoding(maxNodeOffsetToFit, 2 /*min num bytes*/);
}

uint32_t NodeIDCompressionScheme::getNumBytesForEncoding(
    uint64_t maxValToEncode, uint8_t minNumBytes) {
    auto numBytes = minNumBytes;
    while (maxValToEncode > (1ull << (8 * numBytes)) - 2) {
        numBytes <<= 1;
    }
    return numBytes;
}

} // namespace common
} // namespace graphflow
