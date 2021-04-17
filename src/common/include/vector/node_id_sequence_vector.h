#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

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
