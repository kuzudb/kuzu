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

class BaseNodeIDVector : public ValueVector {
protected:
    // The common label field is used with compressions schemes
    // LABEL_0_NODEOFFSET_{2/4/8}_BYTES.
    label_t commonLabel;
    CompressionScheme compressionScheme;

public:
    label_t getLabel() { return commonLabel; }
    void setLabel(label_t label) { this->commonLabel = label; }

    virtual bool isSequence() { return false; }
    virtual void get(uint64_t pos, nodeID_t& nodeID) = 0;
};

class NodeIDVector : public BaseNodeIDVector {

protected:
    uint8_t* nodeIDs;

public:
    // creates a Node ID vector where labels are stores in nodes.
    NodeIDVector(CompressionScheme scheme);
    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeIDVector(label_t label, CompressionScheme scheme);

    ~NodeIDVector() { delete nodeIDs; }

    uint8_t* getNodeIDs() { return nodeIDs; }
    void setNodeIDs(uint8_t* nodeIDs) { this->nodeIDs = nodeIDs; }

    void get(uint64_t pos, nodeID_t& nodeID);
};

class NodeIDSequenceVector : public BaseNodeIDVector {
protected:
    node_offset_t startNodeOffset;

public:
    // creates a Node ID vector where labels are stores in nodes.
    NodeIDSequenceVector();
    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeIDSequenceVector(label_t label);

    bool isSequence() { return true; }

    void get(uint64_t pos, nodeID_t& nodeID) { nodeID.offset = startNodeOffset + pos; }
    void setStartOffset(node_offset_t node_offset) { startNodeOffset = node_offset; }
};

} // namespace common
} // namespace graphflow
