#include "src/processor/include/physical_plan/operator/result_collector.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ResultCollector::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    TableSchema tableSchema;
    for (auto& vectorToCollectInfo : vectorsToCollectInfo) {
        auto dataPos = vectorToCollectInfo.first;
        auto vector =
            resultSet->dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
        vectorsToCollect.push_back(vector);
        tableSchema.appendColumn({!vectorToCollectInfo.second, dataPos.dataChunkPos,
            vectorToCollectInfo.second ? vector->getNumBytesPerValue() : sizeof(overflow_value_t)});
    }
    localQueryResult = make_shared<FactorizedTable>(context->memoryManager, tableSchema);
    sharedQueryResults->appendQueryResult(localQueryResult);
    return resultSet;
}

void ResultCollector::execute(ExecutionContext* context) {
    init(context);
    metrics->executionTime.start();
    while (children[0]->getNextTuples()) {
        // The resultCollector doesn't need to flatten any of the columns, so it always inserts
        // one tuple to the queryResult at a time.
        for (auto i = 0u; i < resultSet->multiplicity; i++) {
            localQueryResult->append(vectorsToCollect);
        }
    }
    metrics->executionTime.stop();
}

void ResultCollector::finalize(ExecutionContext* context) {
    sharedQueryResults->mergeQueryResults();
}

} // namespace processor
} // namespace graphflow
