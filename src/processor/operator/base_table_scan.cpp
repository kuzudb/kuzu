#include "processor/operator/table_scan/base_table_scan.h"

namespace kuzu {
namespace processor {

void BaseTableScan::initFurther(ExecutionContext* context) {
    for (auto i = 0u; i < outVecPositions.size(); ++i) {
        auto outVectorPosition = outVecPositions[i];
        auto outDataChunk = resultSet->dataChunks[outVectorPosition.dataChunkPos];
        auto valueVector = make_shared<ValueVector>(outVecDataTypes[i], context->memoryManager);
        outDataChunk->insert(outVectorPosition.valueVectorPos, valueVector);
        vectorsToScan.push_back(valueVector);
    }
    setMaxMorselSize();
}

bool BaseTableScan::getNextTuplesInternal() {
    auto morsel = getMorsel();
    if (morsel->numTuples == 0) {
        return false;
    }
    morsel->table->scan(vectorsToScan, morsel->startTupleIdx, morsel->numTuples, colIndicesToScan);
    metrics->numOutputTuple.increase(morsel->numTuples);
    return true;
}

} // namespace processor
} // namespace kuzu
