#include "src/processor/include/physical_plan/operator/scan_node_id.h"

namespace graphflow {
namespace processor {

pair<uint64_t, uint64_t> ScanNodeIDSharedState::getNextRangeToRead() {
    auto lck = acquireLock();
    if (currentNodeOffset >= maxNumNodes) {
        return make_pair(currentNodeOffset, currentNodeOffset);
    }
    auto startOffset = currentNodeOffset;
    auto range = min(DEFAULT_VECTOR_CAPACITY, maxNumNodes - currentNodeOffset);
    currentNodeOffset += range;
    return make_pair(startOffset, startOffset + range);
}

shared_ptr<ResultSet> ScanNodeID::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    outValueVector = make_shared<ValueVector>(context->memoryManager, NODE_ID);
    outValueVector->setSequential();
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    return resultSet;
}

bool ScanNodeID::getNextTuples() {
    metrics->executionTime.start();
    auto [startOffset, endOffset] = sharedState->getNextRangeToRead();
    if (startOffset >= endOffset) {
        metrics->executionTime.stop();
        return false;
    }
    auto nodeIDValues = (nodeID_t*)(outValueVector->values);
    auto size = endOffset - startOffset;
    for (auto i = 0u; i < size; ++i) {
        nodeIDValues[i].offset = startOffset + i;
        nodeIDValues[i].label = nodeLabel;
    }
    outDataChunk->state->initOriginalAndSelectedSize(size);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(size);
    return true;
}

} // namespace processor
} // namespace graphflow
