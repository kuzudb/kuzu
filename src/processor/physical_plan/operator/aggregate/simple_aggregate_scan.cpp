#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> SimpleAggregateScan::initResultSet() {
    resultSet = populateResultSet();
    // All aggregate results are materialized in the same dataChunk.
    outDataChunk = resultSet->dataChunks[aggregatesOutputPos[0].dataChunkPos];
    for (auto i = 0u; i < aggregatesOutputPos.size(); i++) {
        outDataChunk->insert(aggregatesOutputPos[i].valueVectorPos,
            make_shared<ValueVector>(context.memoryManager, aggregatesDataType[i]));
    }
    return resultSet;
}

bool SimpleAggregateScan::getNextTuples() {
    metrics->executionTime.start();
    {
        lock_guard<mutex> lock{sharedState->aggregateSharedStateLock};
        if (sharedState->currentGroupOffset >= sharedState->numGroups) {
            metrics->executionTime.stop();
            return false;
        } else {
            // Currently, we assume the aggregate only has one group.
            assert(sharedState->numGroups == 1);
            for (auto i = 0u; i < sharedState->aggregateStates.size(); i++) {
                auto outValueVector =
                    outDataChunk->valueVectors[aggregatesOutputPos[i].valueVectorPos];
                if (sharedState->aggregateStates[i]->isNull) {
                    outValueVector->setNull(0, true);
                } else {
                    memcpy(outValueVector->values, sharedState->aggregateStates[i]->getFinalVal(),
                        TypeUtils::getDataTypeSize(aggregatesDataType[i]));
                }
            }
            outDataChunk->state->initOriginalAndSelectedSize(
                min(DEFAULT_VECTOR_CAPACITY, sharedState->numGroups));
            sharedState->currentGroupOffset += outDataChunk->state->selectedSize;
            metrics->executionTime.stop();
            metrics->numOutputTuple.increase(outDataChunk->state->selectedSize);
            return true;
        }
    }
}

} // namespace processor
} // namespace graphflow
