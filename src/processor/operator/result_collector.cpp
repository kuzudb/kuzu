#include "processor/operator/result_collector.h"

namespace kuzu {
namespace processor {

void FTableSharedState::initTableIfNecessary(
    MemoryManager* memoryManager, unique_ptr<FactorizedTableSchema> tableSchema) {
    lock_guard<mutex> lck{mtx};
    if (table == nullptr) {
        nextTupleIdxToScan = 0u;
        table = make_unique<FactorizedTable>(memoryManager, move(tableSchema));
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
    unique_ptr<FactorizedTableSchema> tableSchema = make_unique<FactorizedTableSchema>();
    for (auto& vectorToCollectInfo : vectorsToCollectInfo) {
        auto dataPos = vectorToCollectInfo.first;
        auto vector =
            resultSet->dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
        vectorsToCollect.push_back(vector);
        tableSchema->appendColumn(
            make_unique<ColumnSchema>(!vectorToCollectInfo.second, dataPos.dataChunkPos,
                vectorToCollectInfo.second ? vector->getNumBytesPerValue() :
                                             (uint32_t)sizeof(overflow_value_t)));
    }
    localTable = make_unique<FactorizedTable>(
        context->memoryManager, make_unique<FactorizedTableSchema>(*tableSchema));
    sharedState->initTableIfNecessary(context->memoryManager, move(tableSchema));
    return resultSet;
}

void ResultCollector::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple()) {
        if (!vectorsToCollect.empty()) {
            for (auto i = 0u; i < resultSet->multiplicity; i++) {
                localTable->append(vectorsToCollect);
            }
        }
    }
    if (!vectorsToCollect.empty()) {
        sharedState->mergeLocalTable(*localTable);
    }
}

} // namespace processor
} // namespace kuzu
