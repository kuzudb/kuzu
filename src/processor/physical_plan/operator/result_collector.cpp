#include "src/processor/include/physical_plan/operator/result_collector.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ResultCollector::initResultSet() {
    resultSet = prevOperator->initResultSet();
    return resultSet;
}

void ResultCollector::reInitialize() {
    prevOperator->reInitialize();
    queryResult->clear();
}

void ResultCollector::execute() {
    metrics->executionTime.start();
    Sink::execute();
    while (prevOperator->getNextTuples()) {
        queryResult->numTuples += resultSet->getNumTuples();
        auto clonedResultSet = make_unique<ResultSet>(resultSet->getNumDataChunks());
        for (auto i = 0u; i < resultSet->getNumDataChunks(); ++i) {
            if (!resultSet->dataChunksMask[i]) {
                continue;
            }
            auto dataChunk = resultSet->dataChunks[i];
            clonedResultSet->insert(i,
                make_shared<DataChunk>(dataChunk->getNumValueVectors(), dataChunk->state->clone()));
        }
        for (auto& dataPos : vectorsToCollectPos) {
            auto vector = resultSet->getValueVector(dataPos);
            clonedResultSet->dataChunks[dataPos.dataChunkPos]->insert(
                dataPos.valueVectorPos, vector->clone());
            if (vector->stringBuffer != nullptr) {
                move(begin(vector->stringBuffer->blocks), end(vector->stringBuffer->blocks),
                    back_inserter(queryResult->bufferBlocks));
            }
        }
        clonedResultSet->multiplicity = resultSet->multiplicity;
        clonedResultSet->dataChunksMask = resultSet->dataChunksMask;
        queryResult->resultSetCollection.push_back(move(clonedResultSet));
        resetStringBuffer();
    }
    metrics->executionTime.stop();
}

void ResultCollector::resetStringBuffer() {
    for (auto& dataChunk : resultSet->dataChunks) {
        for (auto& vector : dataChunk->valueVectors) {
            if (vector->stringBuffer != nullptr) {
                vector->stringBuffer = make_unique<StringBuffer>(*context.memoryManager);
            }
        }
    }
}

} // namespace processor
} // namespace graphflow
