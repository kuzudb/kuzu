
#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

NodeVector::NodeVector(CompressionScheme compressionScheme) {
    this->compressionScheme = compressionScheme;
    this->nodes = new uint8_t[VECTOR_SIZE * getNumBytes(compressionScheme)];
}

NodeVector::NodeVector(label_t label, CompressionScheme compressionScheme)
    : NodeVector(compressionScheme) {
    this->commonLabel = label;
}

NodeSequenceVector::NodeSequenceVector() {
    this->compressionScheme = LABEL_0_NODEOFFSET_8_BYTES;
}

NodeSequenceVector::NodeSequenceVector(label_t label) : NodeSequenceVector() {
    this->commonLabel = label;
}

// The structs below are used to cast the byte array to the appropriate structure.

// node_t { uint32_t uint64_t }

struct node_32_32_t {
    uint32_t label;
    uint32_t nodeOffset;
};

struct node_32_16_t {
    uint32_t label;
    uint16_t nodeOffset;
};

struct node_16_64_t {
    uint16_t label;
    uint64_t nodeOffset;
};

struct node_16_32_t {
    uint16_t label;
    uint32_t nodeOffset;
};

struct node_16_16_t {
    uint16_t label;
    uint16_t nodeOffset;
};

struct node_8_64_t {
    uint8_t label;
    uint64_t nodeOffset;
};

struct node_8_32_t {
    uint8_t label;
    uint32_t nodeOffset;
};

struct node_8_16_t {
    uint8_t label;
    uint16_t nodeOffset;
};

// note: Only called with LABEL_0_NODEOFFSET_8_BYTES.
void NodeVector::set(uint64_t index, node_offset_t node_offset) {
    ((node_offset_t*)nodes)[index] = node_offset;
}

void NodeVector::get(uint64_t index, node_t& node) {
    switch (compressionScheme) {
    case LABEL_0_NODEOFFSET_2_BYTES:
        node.nodeOffset = ((uint16_t*)nodes)[index];
        break;
    case LABEL_0_NODEOFFSET_4_BYTES:
        node.nodeOffset = ((uint32_t*)nodes)[index];
        break;
    case LABEL_0_NODEOFFSET_8_BYTES:
        node.nodeOffset = ((node_offset_t*)nodes)[index];
        break;
    case LABEL_1_NODEOFFSET_2_BYTES:
        node.label = ((node_8_16_t*)nodes)[index].label;
        node.nodeOffset = ((node_8_16_t*)nodes)[index].nodeOffset;
        break;
    case LABEL_1_NODEOFFSET_4_BYTES:
        node.label = ((node_8_32_t*)nodes)[index].label;
        node.nodeOffset = ((node_8_32_t*)nodes)[index].nodeOffset;
        break;
    case LABEL_1_NODEOFFSET_8_BYTES:
        node.label = ((node_8_64_t*)nodes)[index].label;
        node.nodeOffset = ((node_8_64_t*)nodes)[index].nodeOffset;
        break;
    case LABEL_2_NODEOFFSET_2_BYTES:
        node.label = ((node_16_16_t*)nodes)[index].label;
        node.nodeOffset = ((node_16_16_t*)nodes)[index].nodeOffset;
        break;
    case LABEL_2_NODEOFFSET_4_BYTES:
        node.label = ((node_16_32_t*)nodes)[index].label;
        node.nodeOffset = ((node_16_32_t*)nodes)[index].nodeOffset;
        break;
    case LABEL_2_NODEOFFSET_8_BYTES:
        node.label = ((node_16_64_t*)nodes)[index].label;
        node.nodeOffset = ((node_16_64_t*)nodes)[index].nodeOffset;
        break;
    case LABEL_4_NODEOFFSET_2_BYTES:
        node.label = ((node_32_16_t*)nodes)[index].label;
        node.nodeOffset = ((node_32_16_t*)nodes)[index].nodeOffset;
        break;
    case LABEL_4_NODEOFFSET_4_BYTES:
        node.label = ((node_32_32_t*)nodes)[index].label;
        node.nodeOffset = ((node_32_32_t*)nodes)[index].nodeOffset;
        break;
    case LABEL_4_NODEOFFSET_8_BYTES:
        node.label = ((node_t*)nodes)[index].label;
        node.nodeOffset = ((node_t*)nodes)[index].nodeOffset;
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
