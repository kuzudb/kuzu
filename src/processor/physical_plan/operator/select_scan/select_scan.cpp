#include "src/processor/include/physical_plan/operator/select_scan/select_scan.h"

#include <cstring>

namespace graphflow {
namespace processor {

SelectScan::SelectScan(uint32_t totalNumDataChunks, const ResultSet* inResultSet,
    vector<pair<uint32_t, uint32_t>> inDataChunkAndValueVectorsPos, uint32_t outDataChunkSize,
    uint32_t outDataChunkPos, vector<uint32_t> outValueVectorsPos, ExecutionContext& context,
    uint32_t id)
    : PhysicalOperator{SELECT_SCAN, context, id}, totalNumDataChunks{totalNumDataChunks},
      inResultSet{inResultSet}, inDataChunkAndValueVectorsPos{move(inDataChunkAndValueVectorsPos)},
      outDataChunkSize{outDataChunkSize}, outDataChunkPos{outDataChunkPos},
      outValueVectorsPos{move(outValueVectorsPos)}, isFirstExecution{true} {
    resultSet = make_shared<ResultSet>(totalNumDataChunks);
    outDataChunk =
        make_shared<DataChunk>(outDataChunkSize, DataChunkState::getSingleValueDataChunkState());
    for (auto i = 0u; i < this->inDataChunkAndValueVectorsPos.size(); ++i) {
        auto [inDataChunkPos, inValueVectorPos] = this->inDataChunkAndValueVectorsPos[i];
        auto& inValueVector =
            *this->inResultSet->dataChunks[inDataChunkPos]->getValueVector(inValueVectorPos);
        auto outValueVector = make_shared<ValueVector>(
            context.memoryManager, inValueVector.dataType, true /*isSingleValue*/);
        outDataChunk->insert(this->outValueVectorsPos[i], outValueVector);
    }
    resultSet->insert(outDataChunkPos, outDataChunk);
}

void SelectScan::reInitialize() {
    isFirstExecution = true;
}

/**
 * SelectScan assumes all input is flat. Thus, getNextTuples() should be called exactly twice. On
 * the first call SelectScan copies in the flat input tuple. On the second call SelectScan
 * terminates the execution.
 */
void SelectScan::getNextTuples() {
    if (isFirstExecution) {
        isFirstExecution = false;
        for (auto i = 0u; i < inDataChunkAndValueVectorsPos.size(); ++i) {
            auto [inDataChunkPos, inValueVectorPos] = inDataChunkAndValueVectorsPos[i];
            auto& inValueVector =
                *this->inResultSet->dataChunks[inDataChunkPos]->getValueVector(inValueVectorPos);
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
        // We don't need to initialize original size because it is set to 1 in the constructor and
        // never gets updated
        outDataChunk->state->setSelectedSize(1);
        outDataChunk->state->currIdx = 0;
    } else {
        outDataChunk->state->setSelectedSize(0);
        outDataChunk->state->currIdx = -1;
    }
}

} // namespace processor
} // namespace graphflow
