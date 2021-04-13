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

    virtual void readNodeOffset(uint64_t pos, nodeID_t& nodeID);
    virtual void readNodeOffsetAndLabel(uint64_t pos, nodeID_t& nodeID);

    inline int64_t getNumBytesPerValue() override {
        return nodeIDCompressionScheme.getNumTotalBytes();
    }

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

public:
    label_t commonLabel;
    NodeIDCompressionScheme nodeIDCompressionScheme;
    bool isSequence = false;
};

} // namespace common
} // namespace graphflow
