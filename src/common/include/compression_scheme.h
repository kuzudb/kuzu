#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {

class NodeIDCompressionScheme {

public:
    NodeIDCompressionScheme(const vector<label_t>& nbrNodeLabels,
        const vector<uint64_t>& numNodesPerLabel, const uint32_t& numNodeLabels);
    NodeIDCompressionScheme() : NodeIDCompressionScheme(0, 8){};
    NodeIDCompressionScheme(uint32_t numBytesForLabel, uint32_t numBytesForOffset)
        : numBytesForLabel(numBytesForLabel), numBytesForOffset(numBytesForOffset),
          numTotalBytes(numBytesForLabel + numBytesForOffset) {}

    inline uint32_t getNumBytesForLabel() const { return numBytesForLabel; };
    inline uint32_t getNumBytesForOffset() const { return numBytesForOffset; };
    inline uint32_t getNumTotalBytes() const { return numTotalBytes; };

    uint64_t getNodeOffsetNullValue();

private:
    uint32_t getNumBytesForEncoding(const uint64_t& maxValToEncode, const uint8_t& minNumBytes);

private:
    uint32_t numBytesForLabel;
    uint32_t numBytesForOffset;
    uint32_t numTotalBytes;
};

} // namespace common
} // namespace graphflow
