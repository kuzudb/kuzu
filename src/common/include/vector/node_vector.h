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
    NodeIDVector(const string& name, const NodeIDCompressionScheme& nodeIDCompressionScheme)
        : ValueVector{nodeIDCompressionScheme.getNumTotalBytes(), name},
          nodeIDCompressionScheme(nodeIDCompressionScheme){};
    NodeIDVector(
        const string& name, const NodeIDCompressionScheme& compression, const bool& isSequence)
        : ValueVector{compression.getNumTotalBytes(), name}, nodeIDCompressionScheme{compression},
          isSequence{isSequence} {};

    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeIDVector(const string& name, const label_t& label,
        const NodeIDCompressionScheme& nodeIDCompressionScheme)
        : ValueVector{nodeIDCompressionScheme.getNumTotalBytes(), name}, commonLabel{label},
          nodeIDCompressionScheme(nodeIDCompressionScheme) {}
    NodeIDVector(const string& name, const label_t& label,
        const NodeIDCompressionScheme& nodeIDCompressionScheme, const bool& isSequence)
        : ValueVector{nodeIDCompressionScheme.getNumTotalBytes(), name}, commonLabel{label},
          nodeIDCompressionScheme{nodeIDCompressionScheme}, isSequence{isSequence} {};

    label_t getLabel() { return commonLabel; }
    void setLabel(const label_t& label) { this->commonLabel = label; }

    void setCompressionScheme(const NodeIDCompressionScheme& nodeIDCompressionScheme) {
        this->nodeIDCompressionScheme = nodeIDCompressionScheme;
    }

    inline bool getIsSequence() { return isSequence; }
    inline void setIsSequence(const bool& storedSequentially) {
        this->isSequence = storedSequentially;
    }

    virtual void readValue(const uint64_t& pos, nodeID_t& nodeID);

protected:
    NodeIDVector(const string& name, const NodeIDCompressionScheme& nodeIDCompressionScheme,
        const bool& isSequence, const uint64_t& capacity)
        : ValueVector{nodeIDCompressionScheme.getNumTotalBytes() * capacity, name},
          nodeIDCompressionScheme{nodeIDCompressionScheme}, isSequence(isSequence){};

    NodeIDVector(const string& name, const label_t& label,
        const NodeIDCompressionScheme& nodeIDCompressionScheme, const bool& isSequence,
        const uint64_t& capacity)
        : ValueVector{nodeIDCompressionScheme.getNumTotalBytes() * capacity, name},
          commonLabel{label}, nodeIDCompressionScheme{nodeIDCompressionScheme},
          isSequence(isSequence){};

protected:
    // The common label field is used with NodeIDcompressionScheme :
    // LABEL_0_NODEOFFSET_{2/4/8}_BYTES.
    label_t commonLabel;
    NodeIDCompressionScheme nodeIDCompressionScheme;
    bool isSequence;
};

class NodeIDSequenceVector : public NodeIDVector {

public:
    // creates a Node ID vector where labels are stores in nodes.
    NodeIDSequenceVector(const string& name)
        : NodeIDVector{name, NodeIDCompressionScheme(), true, 1} {};
    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeIDSequenceVector(const string& name, const label_t& label)
        : NodeIDVector{name, label, NodeIDCompressionScheme(), true, 1} {};

    void readValue(const uint64_t& pos, nodeID_t& nodeID) {
        nodeID.offset = ((node_offset_t*)values)[0] + pos;
    }
    void setStartOffset(node_offset_t node_offset) { ((node_offset_t*)values)[0] = node_offset; }
};

} // namespace common
} // namespace graphflow
