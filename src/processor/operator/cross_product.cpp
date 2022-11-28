#include "processor/operator/cross_product.h"

namespace kuzu {
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

bool CrossProduct::getNextTuplesInternal() {
    // Note: we should NOT morselize right table scanning (i.e. calling sharedState.getMorsel)
    // because every thread should scan its own table.
    auto table = sharedState->getTable();
    if (table->getNumTuples() == 0) {
        return false;
    }
    if (startIdx == table->getNumTuples()) { // no more to scan from right
        if (!children[0]->getNextTuple()) {  // fetch a new left tuple
            return false;
        }
        startIdx = 0; // reset right table scanning for a new left tuple
    }
    // scan from right table if there is tuple left
    auto maxNumTuplesToScan = table->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    auto numTuplesToScan = min(maxNumTuplesToScan, table->getNumTuples() - startIdx);
    table->scan(vectorsToScan, startIdx, numTuplesToScan, colIndicesToScan);
    startIdx += numTuplesToScan;
    metrics->numOutputTuple.increase(numTuplesToScan);
    return true;
}

} // namespace processor
} // namespace kuzu
