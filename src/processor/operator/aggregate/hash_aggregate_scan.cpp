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
    sharedState->getFactorizedTable()->scan(groupByKeyVectors, startOffset, numRowsToScan,
        groupByKeyVectorsColIdxes);
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

double HashAggregateScan::getProgress(ExecutionContext* /*context*/) const {
    uint64_t totalNumTuples = sharedState->getFactorizedTable()->getNumTuples();
    if (totalNumTuples == 0) {
        return 0.0;
    } else if (sharedState->getCurrentOffset() == totalNumTuples) {
        return 1.0;
    }
    return static_cast<double>(sharedState->getCurrentOffset()) / totalNumTuples;
}

} // namespace processor
} // namespace kuzu
