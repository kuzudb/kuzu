#pragma once

#include <stdint.h>

#include <memory>

#include "src/common/include/compression_scheme.h"
#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

class BaseNodeIDVector : public ValueVector {

public:
    label_t getLabel() { return commonLabel; }
    void setLabel(label_t label) { this->commonLabel = label; }

    void setCompressionScheme(CompressionScheme compressionScheme) {
        this->compressionScheme = compressionScheme;
    }

    virtual void get(uint64_t pos, nodeID_t& nodeID) = 0;

protected:
    // The common label field is used with compressions schemes
    // LABEL_0_NODEOFFSET_{2/4/8}_BYTES.
    label_t commonLabel;
    CompressionScheme compressionScheme;
};

class NodeIDVector : public BaseNodeIDVector {

public:
    // creates a Node ID vector where labels are stores in nodes.
    NodeIDVector(CompressionScheme scheme);
    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeIDVector(label_t label, CompressionScheme scheme);

    void get(uint64_t pos, nodeID_t& value);
};

class NodeIDSequenceVector : public BaseNodeIDVector {

public:
    // creates a Node ID vector where labels are stores in nodes.
    NodeIDSequenceVector();
    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeIDSequenceVector(label_t label);

    void get(uint64_t pos, nodeID_t& nodeID) { nodeID.offset = ((node_offset_t*)values)[0] + pos; }
    void setStartOffset(node_offset_t node_offset) { ((node_offset_t*)values)[0] = node_offset; }
};

} // namespace common
} // namespace graphflow
