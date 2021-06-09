#pragma once

#include <stdint.h>

#include <cassert>
#include <memory>

#include "src/common/include/compression_scheme.h"
#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct NodeIDRepresentation {

    NodeIDRepresentation(
        bool isSequence, label_t commonLabel, const NodeIDCompressionScheme& compressionScheme)
        : isSequence{isSequence}, commonLabel{commonLabel}, compressionScheme{compressionScheme} {}

    const bool isSequence;
    label_t commonLabel;
    NodeIDCompressionScheme compressionScheme;
};

class NodeIDVector : public ValueVector {

public:
    NodeIDVector(label_t commonLabel, const NodeIDCompressionScheme& nodeIDCompressionScheme,
        bool isSequence)
        : NodeIDVector{
              commonLabel, nodeIDCompressionScheme, isSequence, DEFAULT_VECTOR_CAPACITY} {};

    node_offset_t readNodeOffset(uint64_t pos) const override;
    void readNodeID(uint64_t pos, nodeID_t& nodeID) const override;

    inline void setStartOffset(node_offset_t node_offset) {
        assert(representation.isSequence);
        ((node_offset_t*)values)[0] = node_offset;
    }

    inline int64_t getNumBytesPerValue() override {
        return representation.compressionScheme.getNumTotalBytes();
    }

    bool discardNulls();

    bool isSequence() const { return representation.isSequence; }

    shared_ptr<ValueVector> clone() override;

protected:
    NodeIDVector(label_t commonLabel, const NodeIDCompressionScheme& nodeIDCompressionScheme,
        bool isSequence, uint64_t vectorCapacity)
        : ValueVector{vectorCapacity, nodeIDCompressionScheme.getNumTotalBytes(), NODE},
          representation{NodeIDRepresentation(isSequence, commonLabel, nodeIDCompressionScheme)} {};

public:
    NodeIDRepresentation representation;
};

} // namespace common
} // namespace graphflow
