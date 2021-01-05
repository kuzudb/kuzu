#pragma once

#include <cstdint>
#include <mutex>

#include "src/common/include/file_ser_deser_helper.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

class ScanSingleLabel;
class ScanMultiLabel;

class MorselDesc {};

class MorselDescSingleLabelNodeIDs : MorselDesc {
    friend class graphflow::processor::ScanSingleLabel;

public:
    MorselDescSingleLabelNodeIDs(FileDeserHelper& fdsh)
        : nodeLabel{fdsh.read<label_t>()}, maxNodeOffset{fdsh.read<node_offset_t>()} {};

    MorselDescSingleLabelNodeIDs(label_t label, node_offset_t maxNodeOffset)
        : nodeLabel{label}, maxNodeOffset{maxNodeOffset} {}

    node_offset_t getCurrNodeOffset() { return currNodeOffset; }

private:
    void serialize(FileSerHelper& fsh) {
        fsh.write(nodeLabel);
        fsh.write(maxNodeOffset);
    }

private:
    mutex mtx;
    const label_t nodeLabel;
    const node_offset_t maxNodeOffset;
    node_offset_t currNodeOffset{0};
};

class MorselDescMultiLabelNodeIDs : MorselDesc {
    friend class ScanMultiLabel;

public:
    MorselDescMultiLabelNodeIDs(){};
    MorselDescMultiLabelNodeIDs(FileDeserHelper& fdsh) : numLabels{fdsh.read<uint64_t>()} {
        nodeLabels = make_unique<vector<label_t>>(numLabels);
        maxNodeOffsets = make_unique<vector<node_offset_t>>(numLabels);
        for (auto i = 0u; i < numLabels; i++) {
            (*nodeLabels)[i] = fdsh.read<label_t>();
            (*maxNodeOffsets)[i] = fdsh.read<node_offset_t>();
        }
    };

    void addLabel(const label_t& nodeLabel, const node_offset_t& maxNodeOffset) {
        nodeLabels->push_back(nodeLabel);
        maxNodeOffsets->push_back(maxNodeOffset);
        numLabels++;
    }

    node_offset_t getCurrNodeOffset() { return currNodeOffset; }

private:
    void serialize(FileSerHelper& fsh) {
        fsh.write(numLabels);
        for (auto i = 0u; i < numLabels; i++) {
            fsh.write((*nodeLabels)[i]);
            fsh.write((*maxNodeOffsets)[i]);
        }
    }

private:
    mutex mtx;
    uint64_t numLabels{0};
    unique_ptr<vector<label_t>> nodeLabels = make_unique<vector<label_t>>();
    unique_ptr<vector<node_offset_t>> maxNodeOffsets = make_unique<vector<node_offset_t>>();
    uint64_t currPos{0};
    node_offset_t currNodeOffset{0};
};

} // namespace processor
} // namespace graphflow
