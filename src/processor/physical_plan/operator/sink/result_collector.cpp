#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

namespace graphflow {
namespace processor {

void ResultCollector::getNextTuples() {
    executionTime->start();
    prevOperator->getNextTuples();
    auto resultSet = prevOperator->getResultSet();
    queryResult->numTuples += resultSet->getNumTuples() * resultSet->multiplicity;
    if constexpr (ENABLE_DEBUG) {
        resultSetIterator->reset();
        while (resultSetIterator->hasNextTuple()) {
            Tuple tuple(resultSetIterator->dataChunksTypes);
            tuple.multiplicity = resultSet->multiplicity;
            resultSetIterator->getNextTuple(tuple);
            queryResult->tuples.push_back(move(tuple));
        }
    }
    executionTime->stop();
}

} // namespace processor
} // namespace graphflow
