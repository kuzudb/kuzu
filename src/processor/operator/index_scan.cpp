#include "include/index_scan.h"

#include "src/common/include/exception.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> IndexScan::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    auto dataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    dataChunk->state = DataChunkState::getSingleValueDataChunkState();
    outVector = make_shared<ValueVector>(NODE_ID);
    dataChunk->insert(outDataPos.valueVectorPos, outVector);
    hasExecuted = false;
    return resultSet;
}

bool IndexScan::getNextTuples() {
    metrics->executionTime.start();
    if (hasExecuted) {
        metrics->executionTime.stop();
        return false;
    }
    node_offset_t nodeOffset;
    bool isSuccessfulLookup = false;
    switch (hashKey.dataType.typeID) {
    case INT64: {
        isSuccessfulLookup = hashIndex->lookup(transaction, hashKey.val.int64Val, nodeOffset);
    } break;
    default:
        throw RuntimeException("Index look up on data type " +
                               Types::dataTypeToString(hashKey.dataType) + " is not implemented.");
    }
    metrics->executionTime.stop();
    if (isSuccessfulLookup) {
        hasExecuted = true;
        auto nodeIDValues = (nodeID_t*)outVector->values;
        nodeIDValues[0].tableID = tableID;
        nodeIDValues[0].offset = nodeOffset;
        return true;
    } else {
        return false;
    }
}

} // namespace processor
} // namespace graphflow
