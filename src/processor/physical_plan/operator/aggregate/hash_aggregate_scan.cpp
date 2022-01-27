#include "src/processor/include/physical_plan/operator/aggregate/hash_aggregate_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> HashAggregateScan::initResultSet() {
    auto result = BaseAggregateScan::initResultSet();
    for (auto i = 0u; i < groupByKeyVectorsPos.size(); i++) {
        auto valueVector =
            make_shared<ValueVector>(context.memoryManager, groupByKeyVectorDataTypes[i]);
        outDataChunk->insert(groupByKeyVectorsPos[i].valueVectorPos, valueVector);
        groupByKeyVectors.push_back(valueVector.get());
    }
    return result;
}

bool HashAggregateScan::getNextTuples() {
    metrics->executionTime.start();
    if (!sharedState->hasMoreToRead()) {
        metrics->executionTime.stop();
        return false;
    }
    auto [startOffset, endOffset] = sharedState->getNextRangeToRead();
    auto numRowsToScan = endOffset - startOffset;
    for (auto pos = 0u; pos < numRowsToScan; ++pos) {
        auto entry = sharedState->getRow(startOffset + pos);
        auto offset = sizeof(hash_t);
        for (auto& vector : groupByKeyVectors) {
            auto size = TypeUtils::getDataTypeSize(vector->dataType);
            memcpy(vector->values + pos * size, entry + offset, size);
            offset += size;
        }
        for (auto& vector : aggregatesVector) {
            auto size = TypeUtils::getDataTypeSize(vector->dataType);
            auto aggState = (AggregateState*)(entry + offset);
            memcpy(vector->values + pos * size, aggState->getFinalVal(), size);
            offset += aggState->getValSize();
        }
    }
    outDataChunk->state->initOriginalAndSelectedSize(numRowsToScan);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outDataChunk->state->selectedSize);
    return true;
}

} // namespace processor
} // namespace graphflow
