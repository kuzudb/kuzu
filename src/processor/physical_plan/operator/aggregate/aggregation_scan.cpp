#include "src/processor/include/physical_plan/operator/aggregate/aggregation_scan.h"

namespace graphflow {
namespace processor {

void AggregationScan::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    this->resultSet = resultSet;
    // All aggregation results are materialized in the same dataChunk.
    outDataChunkPos = outDataPos[0].dataChunkPos;
    auto outDataChunk = this->resultSet->dataChunks[outDataChunkPos];
    assert(outDataPos.size() == sharedState->dataTypes.size());
    for (auto i = 0u; i < outDataPos.size(); i++) {
        outDataChunk->insert(outDataPos[i].valueVectorPos,
            make_shared<ValueVector>(
                context.memoryManager, sharedState->dataTypes[outDataPos[i].valueVectorPos]));
    }
}

bool AggregationScan::getNextTuples() {
    metrics->executionTime.start();
    auto outDataChunk = this->resultSet->dataChunks[outDataChunkPos];
    {
        lock_guard<mutex> lock{sharedState->aggregationSharedStateLock};
        if (sharedState->currentGroupOffset >= sharedState->numGroups) {
            metrics->executionTime.stop();
            return false;
        } else {
            // Currently, we assume the aggregation only has one group.
            assert(sharedState->numGroups == 1);
            for (auto i = 0u; i < sharedState->aggregationStates.size(); i++) {
                if (sharedState->aggregationStates[i]->isNull) {
                    outDataChunk->valueVectors[i]->setNull(0, true);
                } else {
                    auto outValues = outDataChunk->valueVectors[i]->values;
                    memcpy(outValues, sharedState->aggregationStates[i]->val.get(),
                        TypeUtils::getDataTypeSize(sharedState->dataTypes[i]));
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
