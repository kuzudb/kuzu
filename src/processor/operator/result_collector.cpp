#include "processor/operator/result_collector.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<FTableScanMorsel> FTableSharedState::getMorsel() {
    std::lock_guard<std::mutex> lck{mtx};
    auto numTuplesToScan = std::min(maxMorselSize, table->getNumTuples() - nextTupleIdxToScan);
    auto morsel =
        std::make_unique<FTableScanMorsel>(table.get(), nextTupleIdxToScan, numTuplesToScan);
    nextTupleIdxToScan += numTuplesToScan;
    return morsel;
}

void ResultCollector::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    payloadVectors.reserve(payloadsPos.size());
    for (auto& pos : payloadsPos) {
        payloadVectors.push_back(resultSet->getValueVector(pos).get());
    }
    localTable = std::make_unique<FactorizedTable>(context->memoryManager, tableSchema->copy());
}

void ResultCollector::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        if (!payloadVectors.empty()) {
            for (auto i = 0u; i < resultSet->multiplicity; i++) {
                localTable->append(payloadVectors);
            }
        }
    }
    if (!payloadVectors.empty()) {
        sharedState->mergeLocalTable(*localTable);
    }
}

} // namespace processor
} // namespace kuzu
