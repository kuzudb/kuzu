#pragma once

#include <cstdint>
#include <unordered_set>
#include <utility>
#include <vector>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {

class NodeIDCompressionScheme {

public:
    NodeIDCompressionScheme(const unordered_set<label_t>& nbrNodeLabels,
        const vector<uint64_t>& numNodesPerLabel, const uint32_t& numNodeLabels);
    NodeIDCompressionScheme() : NodeIDCompressionScheme(0, 8){};
    NodeIDCompressionScheme(uint32_t numBytesForLabel, uint32_t numBytesForOffset)
        : numBytesForLabel(numBytesForLabel), numBytesForOffset(numBytesForOffset),
          numTotalBytes(numBytesForLabel + numBytesForOffset) {}

    inline uint32_t getNumBytesForLabel() const { return numBytesForLabel; };
    inline uint32_t getNumBytesForOffset() const { return numBytesForOffset; };
    inline uint32_t getNumTotalBytes() const { return numTotalBytes; };

    uint64_t getNodeOffsetNullValue() const;

private:
    uint32_t numBytesForLabel;
    uint32_t numBytesForOffset;
    uint32_t numTotalBytes;
};

} // namespace common
} // namespace graphflow
