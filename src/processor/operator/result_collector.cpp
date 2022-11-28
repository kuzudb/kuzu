#include "processor/operator/result_collector.h"

namespace kuzu {
namespace processor {

void FTableSharedState::initTableIfNecessary(
    MemoryManager* memoryManager, unique_ptr<FactorizedTableSchema> tableSchema) {
    assert(table == nullptr);
    nextTupleIdxToScan = 0u;
    table = make_unique<FactorizedTable>(memoryManager, std::move(tableSchema));
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
    for (auto [dataPos, _] : payloadsPosAndType) {
        auto vector =
            resultSet->dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
        vectorsToCollect.push_back(vector);
    }
    localTable = make_unique<FactorizedTable>(context->memoryManager, populateTableSchema());
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

void ResultCollector::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->initTableIfNecessary(context->memoryManager, populateTableSchema());
}

unique_ptr<FactorizedTableSchema> ResultCollector::populateTableSchema() {
    unique_ptr<FactorizedTableSchema> tableSchema = make_unique<FactorizedTableSchema>();
    for (auto i = 0u; i < payloadsPosAndType.size(); ++i) {
        auto [dataPos, dataType] = payloadsPosAndType[i];
        tableSchema->appendColumn(make_unique<ColumnSchema>(!isPayloadFlat[i], dataPos.dataChunkPos,
            isPayloadFlat[i] ? Types::getDataTypeSize(dataType) :
                               (uint32_t)sizeof(overflow_value_t)));
    }
    return tableSchema;
}

} // namespace processor
} // namespace kuzu
