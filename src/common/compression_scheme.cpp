#include "src/common/include/compression_scheme.h"

#include <stdexcept>

namespace graphflow {
namespace common {

static uint32_t getNumBytesForEncoding(const uint64_t& maxValToEncode, const uint8_t& minNumBytes);

NodeIDCompressionScheme::NodeIDCompressionScheme(const unordered_set<label_t>& nbrNodeLabels,
    const vector<uint64_t>& numNodesPerLabel, const uint32_t& numNodeLabels)
    : commonLabel(0) {
    auto maxNodeOffsetToFit = 0ull;
    for (auto nodeLabel : nbrNodeLabels) {
        if (numNodesPerLabel[nodeLabel] > maxNodeOffsetToFit) {
            maxNodeOffsetToFit = numNodesPerLabel[nodeLabel];
        }
    }
    auto maxLabelToFit = 1 == nbrNodeLabels.size() ? 0 : numNodeLabels - 1;
    numBytesForLabel =
        maxLabelToFit == 0 ? 0 : getNumBytesForEncoding(maxLabelToFit, 1 /*min num bytes*/);
    numBytesForOffset = getNumBytesForEncoding(maxNodeOffsetToFit, 2 /*min num bytes*/);
    numTotalBytes = numBytesForLabel + numBytesForOffset;
}

uint32_t getNumBytesForEncoding(const uint64_t& maxValToEncode, const uint8_t& minNumBytes) {
    auto numBytes = minNumBytes;
    while (maxValToEncode > (1ull << (8 * numBytes)) - 2) {
        numBytes <<= 1;
    }
    return numBytes;
}

uint64_t NodeIDCompressionScheme::getNodeOffsetNullValue() const {
    switch (numBytesForOffset) {
    case 2:
        return UINT16_MAX;
    case 4:
        return UINT32_MAX;
    case 8:
        return UINT64_MAX;
    default:
        throw std::invalid_argument("Invalid compression scheme in node ID vector.");
    }
}

} // namespace common
} // namespace graphflow
