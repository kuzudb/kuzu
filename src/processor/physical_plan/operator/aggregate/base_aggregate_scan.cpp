#include "src/processor/include/physical_plan/operator/aggregate/base_aggregate_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> BaseAggregateScan::initResultSet() {
    resultSet = populateResultSet();
    outDataChunk = resultSet->dataChunks[aggregatesPos[0].dataChunkPos].get();
    for (auto i = 0u; i < aggregatesPos.size(); i++) {
        auto valueVector = make_shared<ValueVector>(context.memoryManager, aggregatesDataType[i]);
        outDataChunk->insert(aggregatesPos[i].valueVectorPos, valueVector);
        aggregatesVector.push_back(valueVector.get());
    }
    return resultSet;
}

} // namespace processor
} // namespace graphflow
