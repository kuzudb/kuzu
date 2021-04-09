#include "src/processor/include/physical_plan/operator/tuple/data_chunks_iterator.h"

namespace graphflow {
namespace processor {

bool DataChunksIterator::hasNextTuple() {
    return numIteratedTuples < dataChunks.getNumTuples();
}

void DataChunksIterator::reset() {
    tuplePositions.clear();
    for (uint64_t i = 0; i < dataChunks.getNumDataChunks(); i++) {
        auto dataChunk = dataChunks.getDataChunk(i);
        if (dataChunk->isFlat()) {
            tuplePositions.push_back(dataChunk->state->currPos);
        } else {
            tuplePositions.push_back(0);
        }
    }
    numIteratedTuples = 0;
    setDataChunksTypes();
}

bool DataChunksIterator::updateTuplePositions(int64_t chunkIdx) {
    if (dataChunks.getDataChunk(chunkIdx)->isFlat()) {
        return false;
    }
    tuplePositions[chunkIdx] = tuplePositions[chunkIdx] + 1;
    if (tuplePositions[chunkIdx] == dataChunks.getDataChunkState(chunkIdx)->numSelectedValues) {
        tuplePositions[chunkIdx] = 0;
        return false;
    }
    return true;
}

void DataChunksIterator::updateTuplePositions() {
    int64_t lastChunkIdx = dataChunks.getNumDataChunks() - 1;
    while (!updateTuplePositions(lastChunkIdx)) {
        lastChunkIdx = lastChunkIdx - 1;
        if (lastChunkIdx < 0) {
            return;
        }
    }
}

void DataChunksIterator::setDataChunksTypes() {
    dataChunksTypes.clear();
    for (uint64_t i = 0; i < dataChunks.getNumDataChunks(); i++) {
        auto dataChunk = dataChunks.getDataChunk(i);
        for (auto& vector : dataChunk->valueVectors) {
            dataChunksTypes.push_back(vector->getDataType());
        }
    }
}

void DataChunksIterator::getNextTuple(Tuple& tuple) {
    auto valueInTupleIdx = 0;
    nodeID_t nodeID;
    for (uint64_t i = 0; i < dataChunks.getNumDataChunks(); i++) {
        auto dataChunk = dataChunks.getDataChunk(i);
        auto tuplePosition = tuplePositions[i];
        for (auto& vector : dataChunk->valueVectors) {
            auto vectorSelectedValuesPos = vector->getSelectedValuesPos();
            auto selectedTuplePos = vectorSelectedValuesPos[tuplePosition];
            switch (vector->getDataType()) {
            case INT32: {
                tuple.getValue(valueInTupleIdx)
                    ->setInt(vector->getValue<int32_t>(selectedTuplePos));
                break;
            }
            case BOOL: {
                tuple.getValue(valueInTupleIdx)->setBool(vector->getValue<bool>(selectedTuplePos));
                break;
            }
            case DOUBLE: {
                tuple.getValue(valueInTupleIdx)
                    ->setDouble(vector->getValue<double>(selectedTuplePos));
                break;
            }
            case STRING: {
                gf_string_t gfString = vector->getValue<gf_string_t>(selectedTuplePos);
                string value;
                memcpy(&value, vector->getValues() + gfString.overflowPtr, gfString.len);
                tuple.getValue(valueInTupleIdx)->setString(value);
                break;
            }
            case NODE: {
                auto nodeIDVector = static_pointer_cast<NodeIDVector>(vector);
                nodeIDVector->readNodeOffsetAndLabel(selectedTuplePos, nodeID);
                tuple.getValue(valueInTupleIdx)->setNodeID(nodeID);
                break;
            }
            default:
                throw std::invalid_argument(
                    "Unsupported data type " + DataTypeNames[vector->getDataType()]);
            }
            valueInTupleIdx++;
        }
    }
    numIteratedTuples++;
    updateTuplePositions();
}
} // namespace processor
} // namespace graphflow
