
#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

NodeIDVector::NodeIDVector(CompressionScheme compressionScheme) {
    this->compressionScheme = compressionScheme;
    this->nodeIDs = new uint8_t[NODE_SEQUENCE_VECTOR_SIZE * getNumBytes(compressionScheme)];
}

NodeIDVector::NodeIDVector(label_t label, CompressionScheme compressionScheme)
    : NodeIDVector(compressionScheme) {
    this->commonLabel = label;
}

NodeIDSequenceVector::NodeIDSequenceVector() {
    this->compressionScheme = LABEL_0_NODEOFFSET_8_BYTES;
}

NodeIDSequenceVector::NodeIDSequenceVector(label_t label) : NodeIDSequenceVector() {
    this->commonLabel = label;
}

// The structs below are used to cast the byte array to the appropriate structure.

// nodeID_t { uint32_t uint64_t }

struct nodeID_32_32_t {
    uint32_t label;
    uint32_t nodeOffset;
};

struct nodeID_32_16_t {
    uint32_t label;
    uint16_t nodeOffset;
};

struct nodeID_16_64_t {
    uint16_t label;
    uint64_t nodeOffset;
};

struct nodeID_16_32_t {
    uint16_t label;
    uint32_t nodeOffset;
};

struct nodeID_16_16_t {
    uint16_t label;
    uint16_t nodeOffset;
};

struct nodeID_8_64_t {
    uint8_t label;
    uint64_t nodeOffset;
};

struct nodeID_8_32_t {
    uint8_t label;
    uint32_t nodeOffset;
};

struct nodeID_8_16_t {
    uint8_t label;
    uint16_t nodeOffset;
};

void NodeIDVector::get(uint64_t index, nodeID_t& node) {
    switch (compressionScheme) {
    case LABEL_0_NODEOFFSET_2_BYTES:
        node.offset = ((uint16_t*)nodeIDs)[index];
        break;
    case LABEL_0_NODEOFFSET_4_BYTES:
        node.offset = ((uint32_t*)nodeIDs)[index];
        break;
    case LABEL_0_NODEOFFSET_8_BYTES:
        node.offset = ((node_offset_t*)nodeIDs)[index];
        break;
    case LABEL_1_NODEOFFSET_2_BYTES:
        node.label = ((nodeID_8_16_t*)nodeIDs)[index].label;
        node.offset = ((nodeID_8_16_t*)nodeIDs)[index].nodeOffset;
        break;
    case LABEL_1_NODEOFFSET_4_BYTES:
        node.label = ((nodeID_8_32_t*)nodeIDs)[index].label;
        node.offset = ((nodeID_8_32_t*)nodeIDs)[index].nodeOffset;
        break;
    case LABEL_1_NODEOFFSET_8_BYTES:
        node.label = ((nodeID_8_64_t*)nodeIDs)[index].label;
        node.offset = ((nodeID_8_64_t*)nodeIDs)[index].nodeOffset;
        break;
    case LABEL_2_NODEOFFSET_2_BYTES:
        node.label = ((nodeID_16_16_t*)nodeIDs)[index].label;
        node.offset = ((nodeID_16_16_t*)nodeIDs)[index].nodeOffset;
        break;
    case LABEL_2_NODEOFFSET_4_BYTES:
        node.label = ((nodeID_16_32_t*)nodeIDs)[index].label;
        node.offset = ((nodeID_16_32_t*)nodeIDs)[index].nodeOffset;
        break;
    case LABEL_2_NODEOFFSET_8_BYTES:
        node.label = ((nodeID_16_64_t*)nodeIDs)[index].label;
        node.offset = ((nodeID_16_64_t*)nodeIDs)[index].nodeOffset;
        break;
    case LABEL_4_NODEOFFSET_2_BYTES:
        node.label = ((nodeID_32_16_t*)nodeIDs)[index].label;
        node.offset = ((nodeID_32_16_t*)nodeIDs)[index].nodeOffset;
        break;
    case LABEL_4_NODEOFFSET_4_BYTES:
        node.label = ((nodeID_32_32_t*)nodeIDs)[index].label;
        node.offset = ((nodeID_32_32_t*)nodeIDs)[index].nodeOffset;
        break;
    case LABEL_4_NODEOFFSET_8_BYTES:
        node.label = ((nodeID_t*)nodeIDs)[index].label;
        node.offset = ((nodeID_t*)nodeIDs)[index].offset;
        break;
    default:
        throw invalid_argument("Unsupported compression scheme.");
    }
}

uint64_t getNumBytes(CompressionScheme scheme) {
    switch (scheme) {
    case LABEL_0_NODEOFFSET_2_BYTES:
        return 2;
    case LABEL_0_NODEOFFSET_4_BYTES:
        return 4;
    case LABEL_0_NODEOFFSET_8_BYTES:
        return 8;
    case LABEL_1_NODEOFFSET_2_BYTES:
        return 3;
    case LABEL_1_NODEOFFSET_4_BYTES:
        return 5;
    case LABEL_1_NODEOFFSET_8_BYTES:
        return 9;
    case LABEL_2_NODEOFFSET_2_BYTES:
        return 4;
    case LABEL_2_NODEOFFSET_4_BYTES:
        return 6;
    case LABEL_2_NODEOFFSET_8_BYTES:
        return 10;
    case LABEL_4_NODEOFFSET_2_BYTES:
        return 6;
    case LABEL_4_NODEOFFSET_4_BYTES:
        return 8;
    case LABEL_4_NODEOFFSET_8_BYTES:
        return 12;
    default:
        throw invalid_argument("Unsupported compression scheme.");
    }
}

} // namespace common
} // namespace graphflow
