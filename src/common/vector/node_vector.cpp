#include "src/common/include/vector/node_vector.h"

#include <cassert>

namespace graphflow {
namespace common {

void NodeIDVector::readNodeOffset(uint64_t pos, nodeID_t& nodeID) const {
    if (representation.isSequence) {
        nodeID.offset = ((node_offset_t*)values)[0] + pos;
        return;
    }
    auto readOffset = values + (pos * representation.compressionScheme.getNumTotalBytes());
    nodeID.offset = 0;
    memcpy(&nodeID.offset, readOffset + representation.compressionScheme.getNumBytesForLabel(),
        representation.compressionScheme.getNumBytesForOffset());
}

void NodeIDVector::readNodeOffsetAndLabel(uint64_t pos, nodeID_t& nodeID) const {
    if (representation.isSequence) {
        nodeID.offset = ((node_offset_t*)values)[0] + pos;
        nodeID.label = representation.commonLabel;
        return;
    }
    auto readOffset = values + (pos * representation.compressionScheme.getNumTotalBytes());
    if (representation.compressionScheme.getNumBytesForLabel() == 0) {
        nodeID.label = representation.commonLabel;
    } else {
        nodeID.label = 0;
        memcpy(&nodeID.label, readOffset, representation.compressionScheme.getNumBytesForLabel());
    }
    nodeID.offset = 0;
    memcpy(&nodeID.offset, readOffset + representation.compressionScheme.getNumBytesForLabel(),
        representation.compressionScheme.getNumBytesForOffset());
}

bool NodeIDVector::discardNulls() {
    auto nullOffset = representation.compressionScheme.getNodeOffsetNullValue();
    nodeID_t nodeID{};
    if (state->currPos == -1) {
        auto selectedValuesPos = state->selectedValuesPos;
        auto selectedPos = 0u;
        for (auto j = 0u; j < state->size; j++) {
            readNodeOffset(state->selectedValuesPos[j], nodeID);
            if (nodeID.offset != nullOffset) {
                selectedValuesPos[selectedPos++] = j;
            }
        }
        state->size = selectedPos;
        return state->size > 0;
    } else {
        readNodeOffset(state->getCurrSelectedValuesPos(), nodeID);
        return nodeID.offset != nullOffset;
    }
}

shared_ptr<ValueVector> NodeIDVector::clone() {
    auto vectorCapacity = capacity / representation.compressionScheme.getNumTotalBytes();
    auto newVector = make_shared<NodeIDVector>(
        representation.commonLabel, representation.compressionScheme, representation.isSequence);
    memcpy(newVector->nullMask, nullMask, vectorCapacity);
    memcpy(newVector->values, values, capacity);
    return newVector;
}

} // namespace common
} // namespace graphflow
