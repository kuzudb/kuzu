#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
ScanNodeID<IS_OUT_DATACHUNK_FILTERED>::ScanNodeID(shared_ptr<MorselsDesc>& morsel)
    : PhysicalOperator(SCAN), morsel{morsel} {
    resultSet = make_shared<ResultSet>();
    nodeIDVector = make_shared<NodeIDVector>(morsel->label, NodeIDCompressionScheme(), true);
    outDataChunk =
        make_shared<DataChunk>(!IS_OUT_DATACHUNK_FILTERED /* initializeSelectedValuesPos */);
    outDataChunk->append(nodeIDVector);
    resultSet->append(outDataChunk);
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void ScanNodeID<IS_OUT_DATACHUNK_FILTERED>::getNextTuples() {
    {
        unique_lock<mutex> lock{morsel->mtx};
        if (morsel->currNodeOffset >= morsel->numNodes) {
            // no more tuples to scan_node_id.
            nodeIDVector->setStartOffset(0u);
            outDataChunk->state->size = 0u;
        } else {
            nodeIDVector->setStartOffset(morsel->currNodeOffset);
            outDataChunk->state->size =
                min(NODE_SEQUENCE_VECTOR_SIZE, morsel->numNodes - morsel->currNodeOffset);
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
template class ScanNodeID<true>;
template class ScanNodeID<false>;
} // namespace processor
} // namespace graphflow
