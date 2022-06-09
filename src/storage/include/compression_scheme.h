#pragma once

#include <cassert>
#include <cstdint>
#include <unordered_set>
#include <utility>
#include <vector>

#include "src/common/types/include/types_include.h"

namespace graphflow {
namespace common {

class NodeIDCompressionScheme {

public:
    NodeIDCompressionScheme(const unordered_set<label_t>& nbrNodeLabels,
        const vector<node_offset_t>& maxNodeOffsetsPerLabel);
    NodeIDCompressionScheme() : NodeIDCompressionScheme(0, 8){};

private:
    NodeIDCompressionScheme(uint32_t numBytesForLabel, uint32_t numBytesForOffset)
        : numBytesForLabel(numBytesForLabel), numBytesForOffset(numBytesForOffset),
          commonLabel(UINT64_MAX) {}

public:
    inline uint32_t getNumBytesForLabel() const { return numBytesForLabel; };
    inline uint32_t getNumBytesForOffset() const { return numBytesForOffset; };
    inline uint32_t getNumTotalBytes() const { return numBytesForLabel + numBytesForOffset; };
    inline label_t getCommonLabel() const { return commonLabel; };

private:
    static uint32_t getNumBytesForEncoding(uint64_t maxValToEncode, uint8_t minNumBytes);

private:
    uint32_t numBytesForLabel;
    uint32_t numBytesForOffset;
    label_t commonLabel;
};

} // namespace common
} // namespace graphflow
