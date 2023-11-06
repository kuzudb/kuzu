#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

std::unique_ptr<FactorizedTableScanMorsel> FactorizedTableScanSharedState::getMorsel() {
    std::unique_lock lck{mtx};
    auto numTuplesToScan = std::min(maxMorselSize, table->getNumTuples() - nextTupleIdxToScan);
    auto morsel = std::make_unique<FactorizedTableScanMorsel>(nextTupleIdxToScan, numTuplesToScan);
    nextTupleIdxToScan += numTuplesToScan;
    return morsel;
}

void FactorizedTableScan::initLocalStateInternal(
    ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) {
    for (auto& dataPos : info->outputPositions) {
        vectors.push_back(resultSet->getValueVector(dataPos).get());
    }
}

bool FactorizedTableScan::getNextTuplesInternal(ExecutionContext* /*context*/) {
    auto morsel = sharedState->getMorsel();
    if (morsel->numTuples == 0) {
        return false;
    }
    sharedState->table->scan(
        vectors, morsel->startTupleIdx, morsel->numTuples, info->columnIndices);
    metrics->numOutputTuple.increase(morsel->numTuples);
    return true;
}

} // namespace processor
} // namespace kuzu
