#include "src/processor/include/physical_plan/operator/result_collector.h"

namespace graphflow {
namespace processor {

ResultCollector::ResultCollector(vector<DataPos> vectorsToCollectPos,
    unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
    : Sink{move(child), context, id}, vectorsToCollectPos{move(vectorsToCollectPos)} {
    for (auto& dataPos : this->vectorsToCollectPos) {
        dataChunksPosInScope.insert(dataPos.dataChunkPos);
    }
    queryResult = make_unique<QueryResult>(this->vectorsToCollectPos, dataChunksPosInScope);
}

shared_ptr<ResultSet> ResultCollector::initResultSet() {
    resultSet = children[0]->initResultSet();
    return resultSet;
}

void ResultCollector::execute() {
    metrics->executionTime.start();
    Sink::execute();
    while (children[0]->getNextTuples()) {
        queryResult->numTuples += resultSet->getNumTuples(dataChunksPosInScope);
        auto clonedResultSet = make_unique<ResultSet>(resultSet->getNumDataChunks());
        for (auto& pos : dataChunksPosInScope) {
            auto dataChunk = resultSet->dataChunks[pos];
            clonedResultSet->insert(pos,
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
