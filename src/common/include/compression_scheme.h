#pragma once

#include <stdint.h>

#include <utility>
#include <vector>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {

class NodeIDCompressionScheme {

public:
    NodeIDCompressionScheme(const vector<label_t>& nbrNodeLabels,
        const vector<uint64_t>& numNodesPerLabel, const uint32_t& numNodeLabels);
    NodeIDCompressionScheme()
        : numBytesForLabel{0}, numBytesForOffset{8},
          numTotalBytes(numBytesForLabel + numBytesForOffset){};

    inline uint32_t getNumBytesForLabel() const { return numBytesForLabel; };
    inline uint32_t getNumBytesForOffset() const { return numBytesForOffset; };
    inline uint32_t getNumTotalBytes() const { return numTotalBytes; };

private:
    uint32_t getNumBytesForEncoding(const uint64_t& maxValToEncode, const uint8_t& minNumBytes);

private:
    uint32_t numBytesForLabel;
    uint32_t numBytesForOffset;
    uint32_t numTotalBytes;
};

} // namespace common
} // namespace graphflow
