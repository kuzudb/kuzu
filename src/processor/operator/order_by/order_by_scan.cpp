#include "processor/operator/order_by/order_by_scan.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void OrderByScanLocalState::init(
    std::vector<DataPos>& outVectorPos, SortSharedState& sharedState, ResultSet& resultSet) {
    for (auto& dataPos : outVectorPos) {
        vectorsToRead.push_back(resultSet.getValueVector(dataPos).get());
    }
    payloadScanner = std::make_unique<PayloadScanner>(
        sharedState.getMergedKeyBlock(), sharedState.getPayloadTables());
}

void OrderByScan::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    localState->init(outVectorPos, *sharedState, *resultSet);
}

bool OrderByScan::getNextTuplesInternal(ExecutionContext* /*context*/) {
    // If there is no more tuples to read, just return false.
    auto numTuplesRead = localState->scan();
    metrics->numOutputTuple.increase(numTuplesRead);
    return numTuplesRead != 0;
}

} // namespace processor
} // namespace kuzu
