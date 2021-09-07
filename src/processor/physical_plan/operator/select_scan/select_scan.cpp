#include "src/processor/include/physical_plan/operator/select_scan/select_scan.h"

#include <cstring>

namespace graphflow {
namespace processor {

SelectScan::SelectScan(const ResultSet* inResultSet,
    vector<pair<uint64_t, uint64_t>> valueVectorsPosToSelect, ExecutionContext& context,
    uint32_t id)
    : PhysicalOperator{SELECT_SCAN, context, id}, inResultSet{inResultSet},
      valueVectorsPosToSelect{move(valueVectorsPosToSelect)}, isFirstExecution{true} {
    resultSet = make_shared<ResultSet>();
    outDataChunk = make_shared<DataChunk>(DataChunkState::getSingleValueDataChunkState());
    for (auto& [dataChunkPos, valueVectorPos] : this->valueVectorsPosToSelect) {
        auto& inValueVector =
            *this->inResultSet->dataChunks[dataChunkPos]->getValueVector(valueVectorPos);
        shared_ptr<ValueVector> outValueVector;
        if (inValueVector.dataType == NODE) {
            // force decompression given single value;
            auto compressionScheme =
                NodeIDCompressionScheme(sizeof(label_t), sizeof(node_offset_t));
            outValueVector = make_shared<NodeIDVector>(
                compressionScheme, false /*isSequence*/, true /*isSingleValue*/);
        } else {
            outValueVector = make_shared<ValueVector>(
                context.memoryManager, inValueVector.dataType, true /*isSingleValue*/);
        }
        outDataChunk->append(outValueVector);
    }
    resultSet->append(outDataChunk);
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
        for (auto i = 0u; i < this->valueVectorsPosToSelect.size(); ++i) {
            auto dataChunkPos = valueVectorsPosToSelect[i].first;
            auto valueVectorPos = valueVectorsPosToSelect[i].second;
            auto& inValueVector =
                *this->inResultSet->dataChunks[dataChunkPos]->getValueVector(valueVectorPos);
            assert(inValueVector.state->isFlat());
            auto pos = inValueVector.state->getPositionOfCurrIdx();
            if (inValueVector.dataType == NODE) {
                nodeID_t nodeId;
                inValueVector.readNodeID(pos, nodeId);
                // copy label
                memcpy(&outDataChunk->valueVectors[i]->values[0], &nodeId.label, sizeof(label_t));
                // copy offset
                memcpy(&outDataChunk->valueVectors[i]->values[0] + sizeof(label_t), &nodeId.offset,
                    sizeof(node_offset_t));
            } else {
                auto elementSize = TypeUtils::getDataTypeSize(inValueVector.dataType);
                memcpy(&outDataChunk->valueVectors[i]->values[0],
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
