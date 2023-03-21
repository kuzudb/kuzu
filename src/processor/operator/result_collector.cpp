#include "processor/operator/result_collector.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void FTableSharedState::initTableIfNecessary(
    MemoryManager* memoryManager, std::unique_ptr<FactorizedTableSchema> tableSchema) {
    assert(table == nullptr);
    nextTupleIdxToScan = 0u;
    table = std::make_unique<FactorizedTable>(memoryManager, std::move(tableSchema));
}

std::unique_ptr<FTableScanMorsel> FTableSharedState::getMorsel(uint64_t maxMorselSize) {
    std::lock_guard<std::mutex> lck{mtx};
    auto numTuplesToScan = std::min(maxMorselSize, table->getNumTuples() - nextTupleIdxToScan);
    auto morsel =
        std::make_unique<FTableScanMorsel>(table.get(), nextTupleIdxToScan, numTuplesToScan);
    nextTupleIdxToScan += numTuplesToScan;
    return morsel;
}

void ResultCollector::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto [dataPos, _] : payloadsPosAndType) {
        auto vector =
            resultSet->dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
        vectorsToCollect.push_back(vector.get());
    }
    localTable = std::make_unique<FactorizedTable>(context->memoryManager, populateTableSchema());
}

void ResultCollector::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
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

std::unique_ptr<FactorizedTableSchema> ResultCollector::populateTableSchema() {
    std::unique_ptr<FactorizedTableSchema> tableSchema = std::make_unique<FactorizedTableSchema>();
    for (auto i = 0u; i < payloadsPosAndType.size(); ++i) {
        auto [dataPos, dataType] = payloadsPosAndType[i];
        tableSchema->appendColumn(
            std::make_unique<ColumnSchema>(!isPayloadFlat[i], dataPos.dataChunkPos,
                isPayloadFlat[i] ? Types::getDataTypeSize(dataType) :
                                   (uint32_t)sizeof(overflow_value_t)));
    }
    return tableSchema;
}

} // namespace processor
} // namespace kuzu
