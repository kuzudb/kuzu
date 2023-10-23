#include "processor/operator/aggregate/hash_aggregate_scan.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

void HashAggregateScan::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregateScan::initLocalStateInternal(resultSet, context);
    for (auto& dataPos : groupByKeyVectorsPos) {
        auto valueVector = resultSet->getValueVector(dataPos);
        groupByKeyVectors.push_back(valueVector.get());
    }
    groupByKeyVectorsColIdxes.resize(groupByKeyVectors.size());
    iota(groupByKeyVectorsColIdxes.begin(), groupByKeyVectorsColIdxes.end(), 0);
}

bool HashAggregateScan::getNextTuplesInternal(ExecutionContext* /*context*/) {
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
