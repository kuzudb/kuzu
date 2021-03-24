#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
class PhysicalScan : public PhysicalOperator {

public:
    PhysicalScan(shared_ptr<MorselDesc>& morsel) : morsel{morsel} {
        dataChunks = make_shared<DataChunks>();
        nodeIDVector = make_shared<NodeIDSequenceVector>();
        outDataChunk =
            make_shared<DataChunk>(!IS_OUT_DATACHUNK_FILTERED /* initializeSelectedValuesPos */);
        outDataChunk->append(nodeIDVector);
        nodeIDVector->setDataChunkOwner(outDataChunk);
        dataChunks->append(outDataChunk);
    }

    void getNextTuples() override {
        nodeIDVector->setStartOffset(currentMorselStartOffset);
        outDataChunk->size = currentMorselSize;
        outDataChunk->numSelectedValues = currentMorselSize;
        if constexpr (IS_OUT_DATACHUNK_FILTERED) {
            for (auto i = 0u; i < outDataChunk->size; i++) {
                outDataChunk->selectedValuesPos[i] = i;
            }
        }
    }

    bool hasNextMorsel() override {
        unique_lock<mutex> lock{morsel->mtx};
        if (morsel->currNodeOffset >= morsel->numNodes) {
            // no more tuples to scan.
            currentMorselStartOffset = 0u;
            currentMorselSize = 0u;
            return false;
        } else {
            currentMorselStartOffset = morsel->currNodeOffset;
            currentMorselSize = min(
                (uint64_t)NODE_SEQUENCE_VECTOR_SIZE, morsel->numNodes - currentMorselStartOffset);
            morsel->currNodeOffset += currentMorselSize;
            return true;
        }
    }

    shared_ptr<NodeIDSequenceVector>& getNodeVector() { return nodeIDVector; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<PhysicalScan<IS_OUT_DATACHUNK_FILTERED>>(morsel);
    }

protected:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDSequenceVector> nodeIDVector;
    node_offset_t currentMorselStartOffset{-1u};
    uint32_t currentMorselSize{-1u};
    shared_ptr<MorselDesc> morsel;
};

} // namespace processor
} // namespace graphflow
