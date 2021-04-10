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

void NodeIDVector::discardNulls() {
    node_offset_t nullOffset = nodeIDCompressionScheme.getNodeOffsetNullValue();
    nodeID_t nodeID;
    if (owner->currPos == -1) {
        auto selectedPos = 0u;
        for (auto j = 0u; j < owner->numSelectedValues; j++) {
            readNodeOffset(owner->selectedValuesPos[j], nodeID);
            if (nodeID.offset != nullOffset) {
                owner->selectedValuesPos[selectedPos++] = j;
            }
        }
        owner->numSelectedValues = selectedPos;
    } else {
        auto selectedPos = getCurrSelectedValuesPos();
        readNodeOffset(selectedPos, nodeID);
        owner->numSelectedValues = nodeID.offset != nullOffset;
    }
}

} // namespace common
} // namespace graphflow
