#include "src/processor/include/physical_plan/operator/result_scan.h"

#include <cstring>

namespace graphflow {
namespace processor {

void ResultScan::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    outDataChunk = this->resultSet->dataChunks[outDataChunkPos];
    outDataChunk->state = DataChunkState::getSingleValueDataChunkState();
    for (auto i = 0u; i < inDataPoses.size(); ++i) {
        auto [inDataChunkPos, inValueVectorPos] = inDataPoses[i];
        auto& inValueVector =
            *resultSetToCopyFrom->dataChunks[inDataChunkPos]->valueVectors[inValueVectorPos];
        auto outValueVector =
            make_shared<ValueVector>(context.memoryManager, inValueVector.dataType);
        outDataChunk->insert(outValueVectorsPos[i], outValueVector);
    }
}

void ResultScan::reInitialize() {
    isFirstExecution = true;
}

/**
 * ResultScan assumes all input is flat. Thus, getNextTuples() should be called exactly twice. On
 * the first call ResultScan copies in the flat input tuple. On the second call ResultScan
 * terminates the execution.
 */
bool ResultScan::getNextTuples() {
    metrics->executionTime.start();
    if (isFirstExecution) {
        isFirstExecution = false;
        for (auto i = 0u; i < inDataPoses.size(); ++i) {
            auto [inDataChunkPos, inValueVectorPos] = inDataPoses[i];
            auto& inValueVector =
                *this->resultSetToCopyFrom->dataChunks[inDataChunkPos]->getValueVector(
                    inValueVectorPos);
            assert(inValueVector.state->isFlat());
            auto pos = inValueVector.state->getPositionOfCurrIdx();
            auto elementSize = TypeUtils::getDataTypeSize(inValueVector.dataType);
            auto outValueVectorPos = outValueVectorsPos[i];
            if (inValueVector.dataType == NODE && inValueVector.isSequence) {
                auto nodeID = ((nodeID_t*)inValueVector.values)[0];
                nodeID.offset = nodeID.offset + pos;
                memcpy(&outDataChunk->valueVectors[outValueVectorPos]->values[0], &nodeID,
                    elementSize);
            } else {
                memcpy(&outDataChunk->valueVectors[outValueVectorPos]->values[0],
                    inValueVector.values + pos * elementSize, elementSize);
            }
        }
        metrics->executionTime.stop();
        metrics->numOutputTuple.incrementByOne();
        return true;
    } else {
        metrics->executionTime.stop();
        return false;
    }
}

} // namespace processor
} // namespace graphflow
