#include "src/common/include/compression_scheme.h"

#include <stdexcept>

namespace graphflow {
namespace common {

uint64_t getNumBytes(NodeIDCompressionScheme scheme) {
    switch (scheme) {
    case LABEL_0_NODEOFFSET_2_BYTES:
        return 2;
    case LABEL_0_NODEOFFSET_4_BYTES:
        return 4;
    case LABEL_0_NODEOFFSET_8_BYTES:
        return 8;
    case LABEL_1_NODEOFFSET_2_BYTES:
        return 3;
    case LABEL_1_NODEOFFSET_4_BYTES:
        return 5;
    case LABEL_1_NODEOFFSET_8_BYTES:
        return 9;
    case LABEL_2_NODEOFFSET_2_BYTES:
        return 4;
    case LABEL_2_NODEOFFSET_4_BYTES:
        return 6;
    case LABEL_2_NODEOFFSET_8_BYTES:
        return 10;
    case LABEL_4_NODEOFFSET_2_BYTES:
        return 6;
    case LABEL_4_NODEOFFSET_4_BYTES:
        return 8;
    case LABEL_4_NODEOFFSET_8_BYTES:
        return 12;
    default:
        throw std::invalid_argument("Unsupported compression scheme.");
    }
}

NodeIDCompressionScheme getNodeIDCompressionScheme(
    uint32_t numBytesPerlabel, uint32_t numBytesPerOffset) {
    switch (numBytesPerlabel) {
    case 0:
        switch (numBytesPerlabel) {
        case 2:
            return LABEL_0_NODEOFFSET_2_BYTES;
        case 4:
            return LABEL_0_NODEOFFSET_4_BYTES;
        case 8:
            return LABEL_0_NODEOFFSET_8_BYTES;
        }
    case 1:
        switch (numBytesPerlabel) {
        case 2:
            return LABEL_1_NODEOFFSET_2_BYTES;
        case 4:
            return LABEL_1_NODEOFFSET_4_BYTES;
        case 8:
            return LABEL_1_NODEOFFSET_8_BYTES;
        }
    case 2:
        switch (numBytesPerlabel) {
        case 2:
            return LABEL_2_NODEOFFSET_2_BYTES;
        case 4:
            return LABEL_2_NODEOFFSET_4_BYTES;
        case 8:
            return LABEL_2_NODEOFFSET_8_BYTES;
        }
    case 4:
        switch (numBytesPerlabel) {
        case 2:
            return LABEL_4_NODEOFFSET_2_BYTES;
        case 4:
            return LABEL_4_NODEOFFSET_4_BYTES;
        case 8:
            return LABEL_4_NODEOFFSET_8_BYTES;
        }
    default:
        throw std::invalid_argument("Unsupported num bytes per label.");
    }
}

pair<uint32_t, uint32_t> getNumBytesScheme(const vector<label_t>& nbrNodeLabels,
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

uint32_t getNumBytesForEncoding(const uint64_t& val, const uint8_t& minNumBytes) {
    auto numBytes = minNumBytes;
    while (val > (1ull << (8 * numBytes)) - 2) {
        numBytes <<= 1;
    }
    return numBytes;
}

} // namespace common
} // namespace graphflow
