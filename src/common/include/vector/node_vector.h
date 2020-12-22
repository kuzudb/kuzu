#pragma once

#include <stdint.h>

#include <memory>

#include "src/common/include/compression_scheme.h"
#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

class NodeIDVector : public ValueVector {

public:
    // creates a Node ID vector where labels are stores in nodes.
    NodeIDVector(NodeIDCompressionScheme scheme);
    NodeIDVector(NodeIDCompressionScheme scheme, bool isSequence);
    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeIDVector(label_t label, NodeIDCompressionScheme scheme);
    NodeIDVector(label_t label, NodeIDCompressionScheme scheme, bool isSequence);

    label_t getLabel() { return commonLabel; }
    void setLabel(label_t label) { this->commonLabel = label; }

    void setCompressionScheme(NodeIDCompressionScheme compressionScheme) {
        this->compressionScheme = compressionScheme;
    }

    bool getIsSequence() { return isSequence; }
    void setIsSequence(bool storedSequentially) { this->isSequence = storedSequentially; }

    virtual void readValue(uint64_t pos, nodeID_t& value);

protected:
    // The common label field is used with compressions schemes
    // LABEL_0_NODEOFFSET_{2/4/8}_BYTES.
    NodeIDVector() {}

    bool isSequence;
    label_t commonLabel;
    NodeIDCompressionScheme compressionScheme;
};

class NodeIDSequenceVector : public NodeIDVector {

public:
    // creates a Node ID vector where labels are stores in nodes.
    NodeIDSequenceVector();
    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeIDSequenceVector(label_t label);

    void readValue(uint64_t pos, nodeID_t& nodeID) {
        nodeID.offset = ((node_offset_t*)values)[0] + pos;
    }
    void setStartOffset(node_offset_t node_offset) { ((node_offset_t*)values)[0] = node_offset; }
};

} // namespace common
} // namespace graphflow
