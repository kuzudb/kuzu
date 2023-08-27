#include "processor/operator/order_by/top_k_scanner.h"

namespace kuzu {
namespace processor {

void TopKLocalScanState::init(
    std::vector<DataPos>& outVectorPos, TopKSharedState& sharedState, ResultSet& resultSet) {
    scanState = std::make_unique<TopKScanState>();
    sharedState.buffer->initScan(*scanState);
    for (auto& pos : outVectorPos) {
        vectorsToScan.push_back(resultSet.getValueVector(pos).get());
    }
}

void TopKScan::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    localState->init(outVectorPos, *sharedState, *resultSet);
}

bool TopKScan::getNextTuplesInternal(ExecutionContext* context) {
    auto numTuplesRead = localState->scan();
    metrics->numOutputTuple.increase(numTuplesRead);
    return numTuplesRead != 0;
}

} // namespace processor
} // namespace kuzu
