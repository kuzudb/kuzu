#include "src/processor/include/physical_plan/operator/base_table_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> BaseTableScan::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    for (auto i = 0u; i < outVecPositions.size(); ++i) {
        auto outVectorPosition = outVecPositions[i];
        auto outDataChunk = resultSet->dataChunks[outVectorPosition.dataChunkPos];
        auto valueVector = make_shared<ValueVector>(context->memoryManager, outVecDataTypes[i]);
        outDataChunk->insert(outVectorPosition.valueVectorPos, valueVector);
        vectorsToScan.push_back(valueVector);
    }
    setMaxMorselSize();
    return resultSet;
}

bool BaseTableScan::getNextTuples() {
    metrics->executionTime.start();
    auto morsel = getMorsel();
    if (morsel->numTuples == 0) {
        metrics->executionTime.stop();
        return false;
    }
    morsel->table->scan(vectorsToScan, morsel->startTupleIdx, morsel->numTuples);
    metrics->numOutputTuple.increase(morsel->numTuples);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
