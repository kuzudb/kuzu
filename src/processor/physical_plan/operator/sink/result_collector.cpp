#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

namespace graphflow {
namespace processor {

void ResultCollector::getNextTuples() {
    metrics->executionTime.start();
    prevOperator->getNextTuples();
    auto resultSet = prevOperator->getResultSet();
    queryResult->numTuples += resultSet->getNumTuples() * resultSet->multiplicity;
    if (enableProjection) {
        auto clonedResultSet = resultSet->clone();
        queryResult->resultSetCollection.push_back(move(clonedResultSet));
        for (auto& dataChunk : resultSet->dataChunks) {
            for (auto& vector : dataChunk->valueVectors) {
                if (vector->stringBuffer != nullptr) {
                    move(begin(vector->stringBuffer->blocks), end(vector->stringBuffer->blocks),
                        back_inserter(queryResult->bufferBlocks));
                }
            }
        }
    }
    resetStringBuffer();
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
