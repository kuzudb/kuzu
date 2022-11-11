#include "include/index_scan.h"

#include "src/common/include/exception.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> IndexScan::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    auto dataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    dataChunk->state = DataChunkState::getSingleValueDataChunkState();
    outVector = make_shared<ValueVector>(NODE_ID);
    dataChunk->insert(outDataPos.valueVectorPos, outVector);
    indexKeyEvaluator->init(*resultSet, context->memoryManager);
    hasExecuted = false;
    return resultSet;
}

bool IndexScan::getNextTuples() {
    metrics->executionTime.start();
    if (hasExecuted) {
        metrics->executionTime.stop();
        return false;
    }
    indexKeyEvaluator->evaluate();
    auto indexKeyVector = indexKeyEvaluator->resultVector.get();
    assert(indexKeyVector->state->isFlat());
    node_offset_t nodeOffset;
    bool isSuccessfulLookup = pkIndex->lookup(
        transaction, indexKeyVector, indexKeyVector->state->getPositionOfCurrIdx(), nodeOffset);
    metrics->executionTime.stop();
    if (isSuccessfulLookup) {
        hasExecuted = true;
        auto nodeIDValues = (nodeID_t*)outVector->values;
        nodeIDValues[0].tableID = tableID;
        nodeIDValues[0].offset = nodeOffset;
    }
    return isSuccessfulLookup;
}

} // namespace processor
} // namespace graphflow
