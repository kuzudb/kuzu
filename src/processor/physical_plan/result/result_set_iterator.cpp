#include "src/processor/include/physical_plan/result/result_set_iterator.h"

#include <cassert>

namespace graphflow {
namespace processor {

bool ResultSetIterator::hasNextTuple() {
    return numIteratedTuples < resultSet->getNumTuples(dataChunksPosInScope);
}

void ResultSetIterator::reset() {
    tuplePositions.clear();
    for (uint64_t i = 0; i < resultSet->dataChunks.size(); i++) {
        if (!dataChunksPosInScope.contains(i)) {
            tuplePositions.push_back(UINT64_MAX);
            continue;
        }
        auto dataChunk = resultSet->dataChunks[i];
        if (dataChunk->state->isFlat()) {
            tuplePositions.push_back(dataChunk->state->currIdx);
        } else {
            tuplePositions.push_back(0);
        }
    }
    numIteratedTuples = 0;
    numRepeatOfCurrentTuple = 0;
    setDataChunksTypes();
}

bool ResultSetIterator::updateTuplePositions(int64_t chunkIdx) {
    if (!dataChunksPosInScope.contains(chunkIdx) ||
        resultSet->dataChunks[chunkIdx]->state->isFlat()) {
        return false;
    }
    tuplePositions[chunkIdx] = tuplePositions[chunkIdx] + 1;
    if (tuplePositions[chunkIdx] == resultSet->dataChunks[chunkIdx]->state->selectedSize) {
        tuplePositions[chunkIdx] = 0;
        return false;
    }
    return true;
}

void ResultSetIterator::updateTuplePositions() {
    int64_t lastChunkIdx = resultSet->dataChunks.size() - 1;
    while (!updateTuplePositions(lastChunkIdx)) {
        lastChunkIdx = lastChunkIdx - 1;
        if (lastChunkIdx < 0) {
            return;
        }
    }
    numRepeatOfCurrentTuple = 0;
}

void ResultSetIterator::setDataChunksTypes() {
    dataTypes.clear();
    for (auto& dataPos : vectorsToCollectPos) {
        auto dataChunk = resultSet->dataChunks[dataPos.dataChunkPos];
        dataTypes.push_back(dataChunk->valueVectors[dataPos.valueVectorPos]->dataType);
    }
}

void ResultSetIterator::getNextTuple(Tuple& tuple) {
    auto valueInTupleIdx = 0;
    for (auto& dataPos : vectorsToCollectPos) {
        auto tuplePosition = tuplePositions[dataPos.dataChunkPos];
        auto dataChunk = resultSet->dataChunks[dataPos.dataChunkPos];
        auto vector = dataChunk->valueVectors[dataPos.valueVectorPos];
        auto selectedTuplePos = vector->state->selectedPositions[tuplePosition];
        tuple.nullMask[valueInTupleIdx] = vector->isNull(selectedTuplePos);
        if (vector->isNull(selectedTuplePos)) {
            valueInTupleIdx++;
            continue;
        }
        switch (vector->dataType) {
        case INT64: {
            tuple.getValue(valueInTupleIdx)->val.int64Val =
                (((int64_t*)vector->values)[selectedTuplePos]);
        } break;
        case BOOL: {
            tuple.getValue(valueInTupleIdx)->val.booleanVal = vector->values[selectedTuplePos];
        } break;
        case DOUBLE: {
            tuple.getValue(valueInTupleIdx)->val.doubleVal =
                ((double_t*)vector->values)[selectedTuplePos];
        } break;
        case STRING: {
            tuple.getValue(valueInTupleIdx)->val.strVal =
                ((gf_string_t*)vector->values)[selectedTuplePos];
        } break;
        case NODE: {
            nodeID_t nodeID{UINT64_MAX, UINT64_MAX};
            vector->readNodeID(selectedTuplePos, nodeID);
            tuple.getValue(valueInTupleIdx)->val.nodeID = nodeID;
        } break;
        case UNSTRUCTURED: {
            *(tuple.getValue(valueInTupleIdx)) = ((Value*)vector->values)[selectedTuplePos];
        } break;
        case DATE: {
            tuple.getValue(valueInTupleIdx)->val.dateVal =
                ((date_t*)vector->values)[selectedTuplePos];
        } break;
        case TIMESTAMP: {
            tuple.getValue(valueInTupleIdx)->val.timestampVal =
                ((timestamp_t*)vector->values)[selectedTuplePos];
        } break;
        case INTERVAL: {
            tuple.getValue(valueInTupleIdx)->val.intervalVal =
                ((interval_t*)vector->values)[selectedTuplePos];
        } break;
        default:
            assert(false);
        }
        valueInTupleIdx++;
    }
    numIteratedTuples++;
    numRepeatOfCurrentTuple++;
    if (numRepeatOfCurrentTuple == resultSet->multiplicity) {
        updateTuplePositions();
    }
}

} // namespace processor
} // namespace graphflow
