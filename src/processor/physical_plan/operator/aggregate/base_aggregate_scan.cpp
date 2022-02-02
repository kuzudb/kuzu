#include "src/processor/include/physical_plan/operator/aggregate/base_aggregate_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> BaseAggregateScan::initResultSet() {
    resultSet = populateResultSet();
    outDataChunk = resultSet->dataChunks[aggregatesPos[0].dataChunkPos].get();
    for (auto i = 0u; i < aggregatesPos.size(); i++) {
        auto valueVector = make_shared<ValueVector>(context.memoryManager, aggregateDataTypes[i]);
        outDataChunk->insert(aggregatesPos[i].valueVectorPos, valueVector);
        aggregateVectors.push_back(valueVector.get());
    }
    return resultSet;
}

void BaseAggregateScan::writeAggregateResultToVector(
    ValueVector* vector, uint64_t pos, AggregateState* aggregateState) {
    if (aggregateState->isNull) {
        vector->setNull(pos, true);
    } else {
        auto size = TypeUtils::getDataTypeSize(vector->dataType);
        memcpy(vector->values + pos * size, aggregateState->getResult(), size);
    }
}

} // namespace processor
} // namespace graphflow
