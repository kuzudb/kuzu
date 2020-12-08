#pragma once

#include <cstdint>
#include <mutex>

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct MorselDesc {};

struct MorselDescSingleLabelNodeIDs : MorselDesc {
    mutex mtx;
    label_t nodeLabel;
    node_offset_t maxNodeOffset;
    node_offset_t currNodeOffset;

    MorselDescSingleLabelNodeIDs(label_t label, node_offset_t maxNodeOffset)
        : nodeLabel(label), maxNodeOffset(maxNodeOffset), currNodeOffset(0) {}
};

struct MorselDescMultiLabelNodeIDs : MorselDesc {
    mutex mtx;
    uint64_t numLabels;
    label_t* nodeLabel;
    node_offset_t* maxNodeOffset;
    uint64_t currPos;
    node_offset_t currNodeOffset;

    MorselDescMultiLabelNodeIDs(uint64_t num_labels)
        : numLabels(num_labels), nodeLabel(new label_t[numLabels]),
          maxNodeOffset(new node_offset_t[numLabels]), currPos(0), currNodeOffset(0) {}

    ~MorselDescMultiLabelNodeIDs() {
        delete nodeLabel;
        delete maxNodeOffset;
    }
};

} // namespace processor
} // namespace graphflow
