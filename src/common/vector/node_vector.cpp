#include "src/common/include/vector/node_vector.h"

#include <cassert>

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

bool NodeIDVector::discardNulls() {
    auto nullOffset = nodeIDCompressionScheme.getNodeOffsetNullValue();
    nodeID_t nodeID{};
    if (ownerState->currPos == -1) {
        auto selectedPos = 0u;
        for (auto j = 0u; j < ownerState->numSelectedValues; j++) {
            readNodeOffset(ownerState->selectedValuesPos[j], nodeID);
            if (nodeID.offset != nullOffset) {
                ownerState->selectedValuesPos[selectedPos++] = j;
            }
        }
        ownerState->numSelectedValues = selectedPos;
        return ownerState->numSelectedValues > 0;
    } else {
        readNodeOffset(getCurrSelectedValuesPos(), nodeID);
        return nodeID.offset != nullOffset;
    }
}

} // namespace common
} // namespace graphflow
