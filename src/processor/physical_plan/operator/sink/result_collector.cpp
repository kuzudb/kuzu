#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

namespace graphflow {
namespace processor {

void ResultCollector::reInitialize() {
    prevOperator->reInitialize();
    queryResult->clear();
}

void ResultCollector::execute() {
    metrics->executionTime.start();
    while (prevOperator->getNextTuples()) {
        auto resultSet = prevOperator->getResultSet();
        queryResult->numTuples += resultSet->getNumTuples();
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
