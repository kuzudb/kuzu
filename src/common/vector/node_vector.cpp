#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

void NodeIDVector::readNodeOffset(uint64_t pos, nodeID_t& nodeID) {
    auto readOffset = values + (pos * nodeIDCompressionScheme.getNumTotalBytes());
    nodeID.offset = 0;
    memcpy(&nodeID.offset, readOffset + nodeIDCompressionScheme.getNumBytesForLabel(),
        nodeIDCompressionScheme.getNumBytesForOffset());
}

void NodeIDVector::readNodeOffsetAndLabel(uint64_t pos, nodeID_t& nodeID) {
    auto readOffset = values + (pos * nodeIDCompressionScheme.getNumTotalBytes());
    nodeID.label =
        nodeIDCompressionScheme.getNumBytesForLabel() == 0 ? commonLabel : *(label_t*)(readOffset);
    nodeID.offset = *(node_offset_t*)(readOffset + nodeIDCompressionScheme.getNumBytesForLabel());
}

} // namespace common
} // namespace graphflow
