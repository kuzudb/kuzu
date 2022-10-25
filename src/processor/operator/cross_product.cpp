#include "include/cross_product.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> CrossProduct::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto pos : flatDataChunkPositions) {
        auto dataChunk = resultSet->dataChunks[pos];
        dataChunk->state = DataChunkState::getSingleValueDataChunkState();
    }
    for (auto& [pos, dataType] : outVecPosAndTypePairs) {
        auto vector = make_shared<ValueVector>(dataType, context->memoryManager);
        resultSet->dataChunks[pos.dataChunkPos]->insert(pos.valueVectorPos, vector);
        vectorsToScan.push_back(vector);
    }
    startIdx = sharedState->getTable()->getNumTuples();
    return resultSet;
}

bool CrossProduct::getNextTuples() {
    metrics->executionTime.start();
    auto table = sharedState->getTable();
    if (startIdx == table->getNumTuples()) { // no more to scan from right
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        startIdx = 0; // re-scan right table for a new left tuple
    }
    auto maxNumTuplesToScan = table->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    auto numTuplesToScan = min(maxNumTuplesToScan, table->getNumTuples() - startIdx);
    table->scan(vectorsToScan, startIdx, numTuplesToScan, colIndicesToScan);
    startIdx += numTuplesToScan;
    metrics->numOutputTuple.increase(numTuplesToScan);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
