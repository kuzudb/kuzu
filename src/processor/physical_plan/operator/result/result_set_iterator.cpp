#include "src/processor/include/physical_plan/operator/result/result_set_iterator.h"

namespace graphflow {
namespace processor {

bool ResultSetIterator::hasNextTuple() {
    return numIteratedTuples < resultSet.getNumTuples();
}

void ResultSetIterator::reset() {
    tuplePositions.clear();
    for (uint64_t i = 0; i < resultSet.dataChunks.size(); i++) {
        auto dataChunk = resultSet.dataChunks[i];
        if (dataChunk->state->isFlat()) {
            tuplePositions.push_back(dataChunk->state->currPos);
        } else {
            tuplePositions.push_back(0);
        }
    }
    numIteratedTuples = 0;
    setDataChunksTypes();
}

bool ResultSetIterator::updateTuplePositions(int64_t chunkIdx) {
    if (resultSet.dataChunks[chunkIdx]->state->isFlat()) {
        return false;
    }
    tuplePositions[chunkIdx] = tuplePositions[chunkIdx] + 1;
    if (tuplePositions[chunkIdx] == resultSet.dataChunks[chunkIdx]->state->numSelectedValues) {
        tuplePositions[chunkIdx] = 0;
        return false;
    }
    return true;
}

void ResultSetIterator::updateTuplePositions() {
    int64_t lastChunkIdx = resultSet.dataChunks.size() - 1;
    while (!updateTuplePositions(lastChunkIdx)) {
        lastChunkIdx = lastChunkIdx - 1;
        if (lastChunkIdx < 0) {
            return;
        }
    }
}

void ResultSetIterator::setDataChunksTypes() {
    dataChunksTypes.clear();
    for (uint64_t i = 0; i < resultSet.dataChunks.size(); i++) {
        auto dataChunk = resultSet.dataChunks[i];
        for (auto& vector : dataChunk->valueVectors) {
            dataChunksTypes.push_back(vector->dataType);
        }
    }
}

void ResultSetIterator::getNextTuple(Tuple& tuple) {
    auto valueInTupleIdx = 0;
    for (uint64_t i = 0; i < resultSet.dataChunks.size(); i++) {
        auto dataChunk = resultSet.dataChunks[i];
        auto tuplePosition = tuplePositions[i];
        for (auto& vector : dataChunk->valueVectors) {
            auto selectedTuplePos = vector->state->selectedValuesPos[tuplePosition];
            switch (vector->dataType) {
            case INT32: {
                tuple.getValue(valueInTupleIdx)->primitive.int32Val =
                    (((int32_t*)vector->values)[selectedTuplePos]);
            } break;
            case BOOL: {
                tuple.getValue(valueInTupleIdx)->primitive.booleanVal =
                    vector->values[selectedTuplePos];
            } break;
            case DOUBLE: {
                tuple.getValue(valueInTupleIdx)->primitive.doubleVal =
                    ((double_t*)vector->values)[selectedTuplePos];
            } break;
            case STRING: {
                tuple.getValue(valueInTupleIdx)->strVal =
                    ((gf_string_t*)vector->values)[selectedTuplePos];
            } break;
            case NODE: {
                nodeID_t nodeID;
                vector->readNodeID(selectedTuplePos, nodeID);
                tuple.getValue(valueInTupleIdx)->nodeID = nodeID;
                break;
            }
            case UNSTRUCTURED: {
                *(tuple.getValue(valueInTupleIdx)) = ((Value*)vector->values)[selectedTuplePos];
            } break;
            default:
                assert(false);
            }
            valueInTupleIdx++;
        }
    }
    numIteratedTuples++;
    updateTuplePositions();
}

} // namespace processor
} // namespace graphflow
