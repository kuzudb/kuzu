#include "src/processor/include/physical_plan/operator/tuple/data_chunks_iterator.h"

namespace graphflow {
namespace processor {

bool DataChunksIterator::hasNextTuple() {
    return numIteratedTuples < dataChunks.getNumTuples();
}

void DataChunksIterator::initializeTuplePositions() {
    tuplePositions.clear();
    for (uint64_t i = 0; i < dataChunks.getNumDataChunks(); i++) {
        auto dataChunk = dataChunks.getDataChunk(i);
        if (dataChunk->isFlat()) {
            tuplePositions.push_back(dataChunk->currPos);
        } else {
            tuplePositions.push_back(0);
        }
    }
}

bool DataChunksIterator::updateTuplePositions(int64_t chunkIdx) {
    if (dataChunks.getDataChunk(chunkIdx)->isFlat()) {
        return false;
    }
    tuplePositions[chunkIdx] = tuplePositions[chunkIdx] + 1;
    if (tuplePositions[chunkIdx] == dataChunks.getDataChunk(chunkIdx)->getNumTuples()) {
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

void DataChunksIterator::getNextTuple(Tuple& tuple) {
    auto valueInTupleIdx = 0;
    nodeID_t nodeID;
    for (uint64_t i = 0; i < dataChunks.getNumDataChunks(); i++) {
        auto dataChunk = dataChunks.getDataChunk(i);
        auto tuplePosition = tuplePositions[i];
        for (auto& vector : dataChunk->valueVectors) {
            switch (vector->getDataType()) {
            case INT32: {
                tuple.getValue(valueInTupleIdx)->setInt(vector->getValue<int32_t>(tuplePosition));
                break;
            }
            case BOOL: {
                tuple.getValue(valueInTupleIdx)->setBool(vector->getValue<bool>(tuplePosition));
                break;
            }
            case DOUBLE: {
                tuple.getValue(valueInTupleIdx)->setDouble(vector->getValue<double>(tuplePosition));
                break;
            }
            case STRING: {
                gf_string_t gfString = vector->getValue<gf_string_t>(tuplePosition);
                string value;
                memcpy(&value, vector->getValues() + gfString.overflowPtr, gfString.len);
                tuple.getValue(valueInTupleIdx)->setString(value);
                break;
            }
            case NODE: {
                auto nodeIDVector = static_pointer_cast<NodeIDVector>(vector);
                nodeIDVector->readNodeOffsetAndLabel(tuplePosition, nodeID);
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
