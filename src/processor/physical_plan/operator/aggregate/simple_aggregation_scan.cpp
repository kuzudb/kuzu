#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregation_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> SimpleAggregationScan::initResultSet() {
    resultSet = populateResultSet();
    // All aggregation results are materialized in the same dataChunk.
    outDataChunk = resultSet->dataChunks[outDataPos[0].dataChunkPos];
    assert(outDataPos.size() == sharedState->dataTypes.size());
    for (auto i = 0u; i < outDataPos.size(); i++) {
        outDataChunk->insert(outDataPos[i].valueVectorPos,
            make_shared<ValueVector>(context.memoryManager, sharedState->dataTypes[i]));
    }
    return resultSet;
}

bool SimpleAggregationScan::getNextTuples() {
    metrics->executionTime.start();
    {
        lock_guard<mutex> lock{sharedState->aggregationSharedStateLock};
        if (sharedState->currentGroupOffset >= sharedState->numGroups) {
            metrics->executionTime.stop();
            return false;
        } else {
            // Currently, we assume the aggregation only has one group.
            assert(sharedState->numGroups == 1);
            for (auto i = 0u; i < sharedState->aggregationStates.size(); i++) {
                auto outValueVectorPos = outDataPos[i].valueVectorPos;
                if (sharedState->aggregationStates[i]->isNull) {
                    outDataChunk->valueVectors[outValueVectorPos]->setNull(0, true);
                } else {
                    auto outValues = outDataChunk->valueVectors[outValueVectorPos]->values;
                    memcpy(outValues, sharedState->aggregationStates[i]->getFinalVal(),
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
