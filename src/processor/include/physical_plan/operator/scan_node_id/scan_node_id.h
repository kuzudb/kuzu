#pragma once

#include "src/common/include/vector/node_id_sequence_vector.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
class ScanNodeID : public PhysicalOperator {

public:
    ScanNodeID(shared_ptr<MorselsDesc>& morsel) : PhysicalOperator(SCAN), morsel{morsel} {
        dataChunks = make_shared<DataChunks>();
        nodeIDVector = make_shared<NodeIDSequenceVector>();
        outDataChunk =
            make_shared<DataChunk>(!IS_OUT_DATACHUNK_FILTERED /* initializeSelectedValuesPos */);
        outDataChunk->append(nodeIDVector);
        dataChunks->append(outDataChunk);
    }

    void getNextTuples() override {
        {
            unique_lock<mutex> lock{morsel->mtx};
            if (morsel->currNodeOffset >= morsel->numNodes) {
                // no more tuples to scan_node_id.
                nodeIDVector->setStartOffset(0u);
                outDataChunk->state->size = 0u;
            } else {
                nodeIDVector->setStartOffset(morsel->currNodeOffset);
                outDataChunk->state->size = min(
                    (uint64_t)NODE_SEQUENCE_VECTOR_SIZE, morsel->numNodes - morsel->currNodeOffset);
                morsel->currNodeOffset += outDataChunk->state->size;
            }
        }
        outDataChunk->state->numSelectedValues = outDataChunk->state->size;
        if constexpr (IS_OUT_DATACHUNK_FILTERED) {
            for (auto i = 0u; i < outDataChunk->state->size; i++) {
                outDataChunk->state->selectedValuesPos[i] = i;
            }
        }
    }

    shared_ptr<NodeIDSequenceVector>& getNodeVector() { return nodeIDVector; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanNodeID<IS_OUT_DATACHUNK_FILTERED>>(morsel);
    }

protected:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDSequenceVector> nodeIDVector;
    shared_ptr<MorselsDesc> morsel;
};

} // namespace processor
} // namespace graphflow
