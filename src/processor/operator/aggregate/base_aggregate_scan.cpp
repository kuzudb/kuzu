#include "processor/operator/aggregate/base_aggregate_scan.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> BaseAggregateScan::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    for (auto i = 0u; i < aggregatesPos.size(); i++) {
        auto valueVector = make_shared<ValueVector>(aggregateDataTypes[i], context->memoryManager);
        auto outDataChunk = resultSet->dataChunks[aggregatesPos[i].dataChunkPos];
        outDataChunk->insert(aggregatesPos[i].valueVectorPos, valueVector);
        aggregateVectors.push_back(valueVector);
    }
    return resultSet;
}

void BaseAggregateScan::writeAggregateResultToVector(
    ValueVector& vector, uint64_t pos, AggregateState* aggregateState) {
    if (aggregateState->isNull) {
        vector.setNull(pos, true);
    } else {
        memcpy(vector.getData() + pos * vector.getNumBytesPerValue(), aggregateState->getResult(),
            vector.getNumBytesPerValue());
    }
}

} // namespace processor
} // namespace kuzu
