#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"

namespace graphflow {
namespace processor {

ScanNodeID::ScanNodeID(shared_ptr<MorselsDesc>& morsel, ExecutionContext& context, uint32_t id)
    : PhysicalOperator(SCAN, context, id), morsel{morsel} {
    resultSet = make_shared<ResultSet>();
    nodeIDVector = make_shared<NodeIDVector>(morsel->label, NodeIDCompressionScheme(), true);
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(nodeIDVector);
    resultSet->append(outDataChunk);
}

ScanNodeID::ScanNodeID(shared_ptr<MorselsDesc>& morsel, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), SCAN, context, id}, morsel{morsel} {
    resultSet = this->prevOperator->getResultSet();
    nodeIDVector = make_shared<NodeIDVector>(morsel->label, NodeIDCompressionScheme(), true);
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(nodeIDVector);
    resultSet->append(outDataChunk);
}

void ScanNodeID::reInitialize() {
    if (prevOperator) {
        prevOperator->reInitialize();
    }
}

void ScanNodeID::getNextTuples() {
    metrics->executionTime.start();
    if (prevOperator) {
        prevOperator->getNextTuples();
    }
    {
        unique_lock<mutex> lock{morsel->mtx};
        if (morsel->currNodeOffset >= morsel->numNodes) {
            // no more tuples to scan_node_id.
            nodeIDVector->setStartOffset(0u);
            outDataChunk->state->initOriginalAndSelectedSize(0u);
        } else {
            nodeIDVector->setStartOffset(morsel->currNodeOffset);
            outDataChunk->state->initOriginalAndSelectedSize(
                min(DEFAULT_VECTOR_CAPACITY, morsel->numNodes - morsel->currNodeOffset));
            morsel->currNodeOffset += outDataChunk->state->selectedSize;
        }
    }
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outDataChunk->state->selectedSize);
}

} // namespace processor
} // namespace graphflow
