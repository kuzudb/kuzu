#include "src/processor/include/physical_plan/operator/result_collector.h"

namespace graphflow {
namespace processor {

ResultCollector::ResultCollector(vector<pair<DataPos, bool>> vectorsToCollectInfo,
    shared_ptr<SharedQueryResults> sharedQueryResults, unique_ptr<PhysicalOperator> child,
    ExecutionContext& context, uint32_t id)
    : Sink{move(child), context, id}, vectorsToCollectInfo{move(vectorsToCollectInfo)},
      sharedQueryResults{sharedQueryResults} {}

shared_ptr<ResultSet> ResultCollector::initResultSet() {
    resultSet = children[0]->initResultSet();
    TableSchema tableSchema;
    for (auto vectorToCollectInfo : vectorsToCollectInfo) {
        auto dataPos = vectorToCollectInfo.first;
        auto vector =
            resultSet->dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
        vectorsToCollect.push_back(vector);
        tableSchema.appendColumn({!vectorToCollectInfo.second, dataPos.dataChunkPos,
            vectorToCollectInfo.second ? vector->getNumBytesPerValue() : sizeof(overflow_value_t)});
    }
    localQueryResult = make_shared<FactorizedTable>(*context.memoryManager, tableSchema);
    sharedQueryResults->appendQueryResult(localQueryResult);
    return resultSet;
}

void ResultCollector::execute() {
    metrics->executionTime.start();
    Sink::execute();
    while (children[0]->getNextTuples()) {
        // The resultCollector doesn't need to flatten any of the columns, so it always inserts
        // one tuple to the queryResult at a time.
        for (auto i = 0u; i < resultSet->multiplicity; i++) {
            localQueryResult->append(vectorsToCollect);
        }
    }
    metrics->executionTime.stop();
}

void ResultCollector::finalize() {
    sharedQueryResults->mergeQueryResults();
}

} // namespace processor
} // namespace graphflow
