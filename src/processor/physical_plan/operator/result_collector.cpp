#include "src/processor/include/physical_plan/operator/result_collector.h"

namespace graphflow {
namespace processor {

void FTableSharedState::initTableIfNecessary(
    MemoryManager* memoryManager, const TableSchema& tableSchema) {
    lock_guard<mutex> lck{mtx};
    if (table == nullptr) {
        nextTupleIdxToScan = 0u;
        table = make_unique<FactorizedTable>(memoryManager, tableSchema);
    }
}

unique_ptr<FTableScanMorsel> FTableSharedState::getMorsel(uint64_t maxMorselSize) {
    lock_guard<mutex> lck{mtx};
    auto numTuplesToScan = min(maxMorselSize, table->getNumTuples() - nextTupleIdxToScan);
    auto morsel = make_unique<FTableScanMorsel>(table.get(), nextTupleIdxToScan, numTuplesToScan);
    nextTupleIdxToScan += numTuplesToScan;
    return morsel;
}

shared_ptr<ResultSet> ResultCollector::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    TableSchema tableSchema;
    for (auto& vectorToCollectInfo : vectorsToCollectInfo) {
        auto dataPos = vectorToCollectInfo.first;
        auto vector =
            resultSet->dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
        vectorsToCollect.push_back(vector);
        tableSchema.appendColumn({!vectorToCollectInfo.second, dataPos.dataChunkPos,
            vectorToCollectInfo.second ? vector->getNumBytesPerValue() :
                                         (uint32_t)sizeof(overflow_value_t)});
    }
    localTable = make_unique<FactorizedTable>(context->memoryManager, tableSchema);
    sharedState->initTableIfNecessary(context->memoryManager, tableSchema);
    return resultSet;
}

void ResultCollector::execute(ExecutionContext* context) {
    init(context);
    metrics->executionTime.start();
    while (children[0]->getNextTuples()) {
        if (!vectorsToCollect.empty()) {
            for (auto i = 0u; i < resultSet->multiplicity; i++) {
                localTable->append(vectorsToCollect);
            }
        }
    }
    if (!vectorsToCollect.empty()) {
        sharedState->mergeLocalTable(*localTable);
    }
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
