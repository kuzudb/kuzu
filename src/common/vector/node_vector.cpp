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
    if (state->currPos == -1) {
        auto selectedValuesPos = state->selectedValuesPos;
        auto selectedPos = 0u;
        for (auto j = 0u; j < state->numSelectedValues; j++) {
            readNodeOffset(state->selectedValuesPos[j], nodeID);
            if (nodeID.offset != nullOffset) {
                selectedValuesPos[selectedPos++] = j;
            }
        }
        state->numSelectedValues = selectedPos;
        return state->numSelectedValues > 0;
    } else {
        readNodeOffset(state->getCurrSelectedValuesPos(), nodeID);
        return nodeID.offset != nullOffset;
    }
}

} // namespace common
} // namespace graphflow
