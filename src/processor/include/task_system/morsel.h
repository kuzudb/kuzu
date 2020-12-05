#pragma once

#include <cstdint>
#include <mutex>

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct MorselDesc {};

struct MorselSequenceDesc : MorselDesc {
    mutex mtx;
    label_t nodeLabel;
    node_offset_t maxNodeOffset;
    node_offset_t currNodeOffset;

    MorselSequenceDesc(label_t label, node_offset_t nodeOffset)
        : nodeLabel(label), maxNodeOffset(nodeOffset) {}
};

struct MorselMultiLabelSequenceDesc : MorselDesc {
    mutex mtx;
    uint64_t numLabels;
    label_t* nodeLabel;
    node_offset_t* maxNodeOffset;
    uint64_t currPos;
    node_offset_t currNodeOffset;

    MorselMultiLabelSequenceDesc(uint64_t num_labels)
        : numLabels(num_labels), nodeLabel(new label_t[numLabels]),
          maxNodeOffset(new node_offset_t[numLabels]) {}

    ~MorselMultiLabelSequenceDesc() {
        delete nodeLabel;
        delete maxNodeOffset;
    }
};

} // namespace processor
} // namespace graphflow
