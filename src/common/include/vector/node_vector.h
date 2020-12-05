#pragma once

#include <stdint.h>

#include <memory>

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

enum CompressionScheme {
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

uint64_t getNumBytes(CompressionScheme compressionScheme);

class NodeIDVector : public ValueVector {
protected:
    // The common label field is used with compressions schemes
    // LABEL_0_NODEOFFSET_{2/4/8}_BYTES.
    label_t commonLabel;
    CompressionScheme compressionScheme;

public:
    label_t getLabel() { return commonLabel; }
    void setLabel(label_t label) { this->commonLabel = label; };
};

class NodeVector : public NodeIDVector {
protected:
    uint8_t* nodes;

public:
    // creates a Node ID vector where labels are stores in nodes.
    NodeVector(CompressionScheme scheme);
    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeVector(label_t label, CompressionScheme scheme);

    ~NodeVector() { delete nodes; }

    uint8_t* getNodes() { return nodes; }
    void setNodes(uint8_t* nodes) { this->nodes = nodes; }

    void get(uint64_t index, node_t& node);
    void set(uint64_t index, node_offset_t node_offset);
};

class NodeSequenceVector : public NodeIDVector {
protected:
    uint64_t startNodeOffset;

public:
    // creates a Node ID vector where labels are stores in nodes.
    NodeSequenceVector();
    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeSequenceVector(label_t label);

    void get(uint64_t index, node_t& node) { node.nodeOffset = startNodeOffset + index; }

    void set(node_offset_t node_offset) { startNodeOffset = node_offset; }
};

} // namespace common
} // namespace graphflow
