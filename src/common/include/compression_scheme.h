#pragma once

#include <cassert>
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

private:
    NodeIDCompressionScheme(uint32_t numBytesForLabel, uint32_t numBytesForOffset)
        : numBytesForLabel(numBytesForLabel), numBytesForOffset(numBytesForOffset),
          numTotalBytes(numBytesForLabel + numBytesForOffset), commonLabel(UINT64_MAX) {}

public:
    inline uint32_t getNumBytesForLabel() const { return numBytesForLabel; };
    inline uint32_t getNumBytesForOffset() const { return numBytesForOffset; };
    inline uint32_t getNumTotalBytes() const { return numTotalBytes; };
    inline label_t getCommonLabel() const { return commonLabel; };

    void setCommonLabel(label_t label) {
        assert(numBytesForLabel == 0);
        commonLabel = label;
    };

private:
    uint32_t numBytesForLabel;
    uint32_t numBytesForOffset;
    uint32_t numTotalBytes;
    label_t commonLabel;
};

} // namespace common
} // namespace graphflow
