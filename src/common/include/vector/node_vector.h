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
    NodeIDVector(const NodeIDCompressionScheme& nodeIDCompressionScheme)
        : NodeIDVector{0, nodeIDCompressionScheme, false} {}
    NodeIDVector(const NodeIDCompressionScheme& nodeIDCompressionScheme, bool isSequence)
        : NodeIDVector{0, nodeIDCompressionScheme, isSequence} {}

    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeIDVector(label_t commonLabel, const NodeIDCompressionScheme& nodeIDCompressionScheme)
        : NodeIDVector{commonLabel, nodeIDCompressionScheme, false} {}

    NodeIDVector(label_t commonLabel, const NodeIDCompressionScheme& nodeIDCompressionScheme,
        bool isSequence)
        : NodeIDVector{commonLabel, nodeIDCompressionScheme, isSequence, MAX_VECTOR_SIZE} {};

    virtual ~NodeIDVector() = default;

    inline label_t getCommonLabel() const { return commonLabel; }
    inline void setCommonLabel(label_t commonLabel) { this->commonLabel = commonLabel; }

    inline void setCompressionScheme(const NodeIDCompressionScheme& nodeIDCompressionScheme) {
        this->nodeIDCompressionScheme = nodeIDCompressionScheme;
    }
    inline NodeIDCompressionScheme& getCompressionScheme() { return nodeIDCompressionScheme; }

    inline bool getIsSequence() const { return isSequence; }
    inline void setIsSequence(bool storedSequentially) { this->isSequence = storedSequentially; }

    inline int64_t getNumBytesPerValue() override {
        return nodeIDCompressionScheme.getNumTotalBytes();
    }

    virtual void readNodeOffset(uint64_t pos, nodeID_t& nodeID) override;
    virtual void readNodeOffsetAndLabel(uint64_t pos, nodeID_t& nodeID) override;

    bool discardNulls();

protected:
    NodeIDVector(const NodeIDCompressionScheme& nodeIDCompressionScheme, bool isSequence,
        uint64_t vectorCapacity)
        : NodeIDVector{0, nodeIDCompressionScheme, isSequence, vectorCapacity} {}

    NodeIDVector(label_t commonLabel, const NodeIDCompressionScheme& nodeIDCompressionScheme,
        bool isSequence, uint64_t vectorCapacity)
        : ValueVector{vectorCapacity, nodeIDCompressionScheme.getNumTotalBytes(), NODE},
          commonLabel{commonLabel}, nodeIDCompressionScheme{nodeIDCompressionScheme},
          isSequence{isSequence} {};

protected:
    label_t commonLabel;
    NodeIDCompressionScheme nodeIDCompressionScheme;
    bool isSequence = false;
};

class NodeIDSequenceVector : public NodeIDVector {

public:
    // creates a Node ID vector where labels are stores in nodes.
    NodeIDSequenceVector() : NodeIDVector{NodeIDCompressionScheme(), true, 1} {};
    //  Creates a Node ID vector where the label is factored out and
    // stores in the label field and nodes contains node offsets only.
    NodeIDSequenceVector(label_t commonLabel)
        : NodeIDVector{commonLabel, NodeIDCompressionScheme(), true, 1} {};

    void readNodeOffset(uint64_t pos, nodeID_t& nodeID) override {
        nodeID.offset = ((node_offset_t*)values)[0] + pos;
    }

    void readNodeOffsetAndLabel(uint64_t pos, nodeID_t& nodeID) override {
        nodeID.offset = ((node_offset_t*)values)[0] + pos;
        nodeID.label = commonLabel;
    }

    void setStartOffset(node_offset_t node_offset) { ((node_offset_t*)values)[0] = node_offset; }
};

} // namespace common
} // namespace graphflow
