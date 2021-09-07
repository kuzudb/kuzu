#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"

namespace graphflow {
namespace processor {

ScanNodeID::ScanNodeID(uint32_t totalNumDataChunks, uint32_t outDataChunkSize,
    uint32_t outDataChunkPos, uint32_t outValueVectorPos, shared_ptr<MorselsDesc>& morsel,
    ExecutionContext& context, uint32_t id)
    : PhysicalOperator(SCAN, context, id), totalNumDataChunks{totalNumDataChunks},
      outDataChunkSize{outDataChunkSize}, outDataChunkPos{outDataChunkPos},
      outValueVectorPos{outValueVectorPos}, morsel{morsel} {
    resultSet = make_shared<ResultSet>(totalNumDataChunks);
    initResultSet(outDataChunkSize);
}

ScanNodeID::ScanNodeID(uint32_t outDataChunkSize, uint32_t outDataChunkPos,
    uint32_t outValueVectorPos, shared_ptr<MorselsDesc>& morsel,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), SCAN, context, id}, outDataChunkSize{outDataChunkSize},
      outDataChunkPos{outDataChunkPos}, outValueVectorPos{outValueVectorPos}, morsel{morsel} {
    resultSet = this->prevOperator->getResultSet();
    initResultSet(outDataChunkSize);
}

void ScanNodeID::initResultSet(uint32_t outDataChunkSize) {
    outValueVector = make_shared<ValueVector>(
        context.memoryManager, NODE, true /* isSingleValue */, true /* isSequence */);
    outDataChunk = make_shared<DataChunk>(outDataChunkSize);
    outDataChunk->insert(outValueVectorPos, outValueVector);
    resultSet->insert(outDataChunkPos, outDataChunk);
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
        auto nodeIDValues = (nodeID_t*)(outValueVector->values);
        // Fill the first nodeID in the sequence.
        if (morsel->currNodeOffset >= morsel->numNodes) {
            // no more tuples to scan_node_id.
            nodeIDValues[0].label = morsel->label;
            nodeIDValues[0].offset = 0;
            outDataChunk->state->initOriginalAndSelectedSize(0u);
        } else {
            nodeIDValues[0].label = morsel->label;
            nodeIDValues[0].offset = morsel->currNodeOffset;
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
