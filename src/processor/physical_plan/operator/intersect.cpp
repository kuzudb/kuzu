#include "src/processor/include/physical_plan/operator/intersect.h"

#include <iostream>

using namespace graphflow::common;

namespace graphflow {
namespace processor {

bool IntersectProbe::getChildNextTuples() const {
    if (childProbe) {
        if (!childProbe->probe()) {
            return false;
        }
    } else {
        if (!nextOp->getNextTuples()) {
            return false;
        }
    }
    return true;
}

void IntersectProbe::probeHT(const nodeID_t& probeKey) const {
    assert(probeKeyVector->state->isFlat());
    joinSharedState->getHashTable()->probe(
        *probeKeyVector, probeState->probedTuples.get(), *probeState->probeSelVector);
    if (probeState->probeSelVector->selectedSize == 0) {
        probeState->matchedSelVector->selectedSize = 0;
        probeState->nextMatchedTupleIdx = 0;
    }
    auto numMatchedTuples = 0;
    while (true) {
        if (numMatchedTuples == DEFAULT_VECTOR_CAPACITY || probeState->probedTuples[0] == nullptr) {
            break;
        }
        auto currentTuple = probeState->probedTuples[0];
        probeState->matchedTuples[numMatchedTuples] = currentTuple;
        numMatchedTuples += *(nodeID_t*)currentTuple == probeKey;
        probeState->probedTuples[0] = *joinSharedState->getHashTable()->getPrevTuple(currentTuple);
    }
    probeState->matchedSelVector->selectedSize = numMatchedTuples;
    probeState->nextMatchedTupleIdx = 0;
}

bool IntersectProbe::probe() {
    if (probeState->nextMatchedTupleIdx < probeState->matchedSelVector->selectedSize) {
        fetchFromHT();
        return true;
    }
    if (!getChildNextTuples()) {
        return false;
    }
    auto probeKey =
        ((nodeID_t*)probeKeyVector->values)[probeKeyVector->state->getPositionOfCurrIdx()];
    if (probeKey == prevProbeKey) {
        probeState->nextMatchedTupleIdx = 0;
    } else {
        prevProbeKey = probeKey;
        probeHT(probeKey);
    }
    fetchFromHT();
    return true;
}

void IntersectProbe::fetchFromHT() {
    if (probeState->matchedSelVector->selectedSize == 0) {
        probedResult.numElements = 0;
        return;
    }
    probedResult = *(overflow_value_t*)(probeState->matchedTuples[probeState->nextMatchedTupleIdx] +
                                        16); // TODO(Guodong): This should not be a magic number.
    probeState->nextMatchedTupleIdx++;
}

shared_ptr<ResultSet> Intersect::init(graphflow::processor::ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    auto outputDataChunk = resultSet->dataChunks[outputVectorPos.dataChunkPos];
    outputVector = make_shared<ValueVector>(NODE_ID);
    outputDataChunk->insert(0, outputVector);
    prober->init(*resultSet);
    return resultSet;
}

void Intersect::twoWayIntersect(
    uint8_t* leftValues, uint64_t leftSize, uint8_t* rightValues, uint64_t rightSize) {
    sel_t leftPosition = 0;
    sel_t rightPosition = 0;
    sel_t outputValuePosition = 0;
    auto outputValues = (nodeID_t*)outputVector->values;
    while (leftPosition < leftSize && rightPosition < rightSize) {
        auto leftVal = ((nodeID_t*)leftValues)[leftPosition];
        auto rightVal = ((nodeID_t*)rightValues)[rightPosition];
        if (leftVal.offset < rightVal.offset) {
            leftPosition++;
        } else if (leftVal.offset > rightVal.offset) {
            rightPosition++;
        } else {
            leftPosition++;
            rightPosition++;
            outputValues[outputValuePosition++] = leftVal;
        }
    }
    outputVector->state->selVector->selectedSize = outputValuePosition;
}

static uint32_t findSmallest(const vector<overflow_value_t*>& probeResults) {
    assert(probeResults.size() >= 2);
    uint32_t smallestSize = probeResults[0]->numElements;
    uint32_t result = 0;
    for (auto i = 1u; i < probeResults.size(); i++) {
        if (probeResults[i]->numElements < smallestSize) {
            smallestSize = probeResults[i]->numElements;
            result = i;
        }
    }
    return result;
}

void Intersect::kWayIntersect() {
    auto smallestIdx = findSmallest(probeResults);
    if (smallestIdx != 0) {
        swap(probeResults[smallestIdx], probeResults[0]);
    }
    twoWayIntersect(probeResults[0]->value, probeResults[0]->numElements, probeResults[1]->value,
        probeResults[1]->numElements);
    uint64_t nextToIntersect = 2;
    while (nextToIntersect < probeResults.size()) {
        twoWayIntersect(outputVector->values, outputVector->state->selVector->selectedSize,
            probeResults[nextToIntersect]->value, probeResults[nextToIntersect]->numElements);
        nextToIntersect++;
    }
}

bool Intersect::getNextTuples() {
    metrics->executionTime.start();
    do {
        auto probeResult = prober->probe();
        if (!probeResult) {
            metrics->executionTime.stop();
            return false;
        }
        kWayIntersect();
    } while (outputVector->state->selVector->selectedSize == 0);
    metrics->numOutputTuple.increase(outputVector->state->selVector->selectedSize);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow