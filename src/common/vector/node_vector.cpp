#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

void NodeIDVector::readValue(const uint64_t& pos, nodeID_t& nodeID) {
    auto readOffset = values + (pos * nodeIDCompressionScheme.getNumTotalBytes());
    nodeID.label = 0;
    memcpy(&nodeID.label, readOffset, nodeIDCompressionScheme.getNumBytesForLabel());
    nodeID.offset = 0;
    memcpy(&nodeID.offset, readOffset + nodeIDCompressionScheme.getNumBytesForLabel(),
        nodeIDCompressionScheme.getNumBytesForOffset());
}

} // namespace common
} // namespace graphflow
