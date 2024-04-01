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

void OrderByScan::initGlobalStateInternal(ExecutionContext* /*context*/) {
    for (auto& table : sharedState->getPayloadTables()) {
        sharedState->numTuples += table->getTotalNumFlatTuples();
    }
}

void OrderByScan::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    localState->init(outVectorPos, *sharedState, *resultSet);
}

bool OrderByScan::getNextTuplesInternal(ExecutionContext* /*context*/) {
    // If there is no more tuples to read, just return false.
    auto numTuplesRead = localState->scan();
    metrics->numOutputTuple.increase(numTuplesRead);
    sharedState->numTuplesRead += numTuplesRead;
    return numTuplesRead != 0;
}

double OrderByScan::getProgress(ExecutionContext* /*context*/) const {
    if (sharedState->numTuples == 0) {
        return 0.0;
    } else if (sharedState->numTuplesRead == sharedState->numTuples) {
        return 1.0;
    }
    return static_cast<double>(sharedState->numTuplesRead) / sharedState->numTuples;
}

} // namespace processor
} // namespace kuzu
