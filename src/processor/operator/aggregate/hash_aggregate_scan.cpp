#include "processor/operator/aggregate/hash_aggregate_scan.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> HashAggregateScan::init(ExecutionContext* context) {
    auto result = BaseAggregateScan::init(context);
    for (auto i = 0u; i < groupByKeyVectorsPos.size(); i++) {
        auto valueVector =
            make_shared<ValueVector>(groupByKeyVectorDataTypes[i], context->memoryManager);
        auto outDataChunk = resultSet->dataChunks[groupByKeyVectorsPos[i].dataChunkPos];
        outDataChunk->insert(groupByKeyVectorsPos[i].valueVectorPos, valueVector);
        groupByKeyVectors.push_back(valueVector);
    }
    groupByKeyVectorsColIdxes.resize(groupByKeyVectors.size());
    iota(groupByKeyVectorsColIdxes.begin(), groupByKeyVectorsColIdxes.end(), 0);
    return result;
}

bool HashAggregateScan::getNextTuplesInternal() {
    auto [startOffset, endOffset] = sharedState->getNextRangeToRead();
    if (startOffset >= endOffset) {
        return false;
    }
    auto numRowsToScan = endOffset - startOffset;
    sharedState->getFactorizedTable()->scan(
        groupByKeyVectors, startOffset, numRowsToScan, groupByKeyVectorsColIdxes);
    for (auto pos = 0u; pos < numRowsToScan; ++pos) {
        auto entry = sharedState->getRow(startOffset + pos);
        auto offset = sharedState->getFactorizedTable()->getTableSchema()->getColOffset(
            groupByKeyVectors.size());
        for (auto& vector : aggregateVectors) {
            auto aggState = (AggregateState*)(entry + offset);
            writeAggregateResultToVector(*vector, pos, aggState);
            offset += aggState->getStateSize();
        }
    }
    metrics->numOutputTuple.increase(numRowsToScan);
    return true;
}

} // namespace processor
} // namespace kuzu
