#include "src/storage/include/compression_scheme.h"

#include <stdexcept>

namespace graphflow {
namespace common {

NodeIDCompressionScheme::NodeIDCompressionScheme(const unordered_set<label_t>& nbrNodeLabels,
    const vector<node_offset_t>& maxNodeOffsetsPerLabel)
    : commonLabel(0) {
    uint64_t maxNumNodeOffsetToFit = 0;
    for (auto nodeLabel : nbrNodeLabels) {
        if (maxNodeOffsetsPerLabel[nodeLabel] + 1 > maxNumNodeOffsetToFit) {
            maxNumNodeOffsetToFit = maxNodeOffsetsPerLabel[nodeLabel] + 1;
        }
    }
    if (nbrNodeLabels.size() == 1) {
        numBytesForLabel = 0;
        commonLabel = *nbrNodeLabels.begin();
    } else {
        numBytesForLabel =
            getNumBytesForEncoding(maxNodeOffsetsPerLabel.size() - 1, 1 /* min num bytes */);
    }
    numBytesForOffset = getNumBytesForEncoding(maxNumNodeOffsetToFit, 2 /*min num bytes*/);
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
