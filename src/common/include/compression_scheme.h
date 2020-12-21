#pragma once

#include <stdint.h>

#include <utility>
#include <vector>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {

enum NodeIDCompressionScheme {
    LABEL_0_NODEOFFSET_2_BYTES,
    LABEL_0_NODEOFFSET_4_BYTES,
    LABEL_0_NODEOFFSET_8_BYTES,
    LABEL_1_NODEOFFSET_2_BYTES,
    LABEL_1_NODEOFFSET_4_BYTES,
    LABEL_1_NODEOFFSET_8_BYTES,
    LABEL_2_NODEOFFSET_2_BYTES,
    LABEL_2_NODEOFFSET_4_BYTES,
    LABEL_2_NODEOFFSET_8_BYTES,
    LABEL_4_NODEOFFSET_2_BYTES,
    LABEL_4_NODEOFFSET_4_BYTES,
    LABEL_4_NODEOFFSET_8_BYTES,
};

uint64_t getNumBytes(NodeIDCompressionScheme compressionScheme);

NodeIDCompressionScheme getNodeIDCompressionScheme(
    uint32_t numBytesPerlabel, uint32_t numBytesPerOffset);

pair<uint32_t, uint32_t> getNumBytesScheme(const vector<label_t>& nbrNodeLabels,
    const vector<uint64_t>& numNodesPerLabel, const uint32_t& numNodeLabels);

uint32_t getNumBytesForEncoding(const uint64_t& value, const uint8_t& minNumBytes);

} // namespace common
} // namespace graphflow
