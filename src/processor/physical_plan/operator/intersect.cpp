#include "src/processor/include/physical_plan/operator/intersect.h"

using namespace graphflow::common;

#include <iostream>

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Intersect::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto i = 0u; i < probeSideKeyVectorsPos.size(); i++) {
        probeSideKeyVectors[i] = resultSet->getValueVector(probeSideKeyVectorsPos[i]);
    }
    auto outputDataChunk = resultSet->dataChunks[outputVectorPos.dataChunkPos];
    outputVector = make_shared<ValueVector>(NODE_ID);
    outputDataChunk->insert(0, outputVector);
    return resultSet;
}

void Intersect::probe() {
    //    cout << "Probe keys [";
    for (auto i = 0u; i < sharedHTs.size(); i++) {
        auto key = ((nodeID_t*)probeSideKeyVectors[i]
                        ->values)[probeSideKeyVectors[i]->state->getPositionOfCurrIdx()]
                       .offset;
        probedLists[i] = sharedHTs[i]->probe(key);
        //        cout << key << " ";
    }
    //    cout << "]" << endl;
}

uint64_t Intersect::twoWayIntersect(uint8_t* leftValues, uint64_t leftSize, uint8_t* rightValues,
    uint64_t rightSize, uint8_t* output, label_t keyLabel) {
    sel_t leftPosition = 0;
    sel_t rightPosition = 0;
    uint64_t outputValuePosition = 0;
    auto outputValues = (nodeID_t*)output;
    auto leftNodeOffsets = (node_offset_t*)leftValues;
    auto rightNodeOffsets = (node_offset_t*)rightValues;
    //    cout << "Intersect:" << endl;
    //    cout << "L(" << leftSize << "): [";
    //    for (auto i = 0u; i < leftSize; i++) {
    //        cout << leftNodeOffsets[i] << ",";
    //    }
    //    cout << "]" << endl;
    //    cout << "R(" << rightSize << "): [";
    //    for (auto i = 0u; i < rightSize; i++) {
    //        cout << rightNodeOffsets[i] << ",";
    //    }
    //    cout << "]" << endl;
    while (leftPosition < leftSize && rightPosition < rightSize) {
        auto leftVal = leftNodeOffsets[leftPosition];
        auto rightVal = rightNodeOffsets[rightPosition];
        if (leftVal < rightVal) {
            leftPosition++;
        } else if (leftVal > rightVal) {
            rightPosition++;
        } else {
            leftPosition++;
            rightPosition++;
            outputValues[outputValuePosition].label = keyLabel;
            outputValues[outputValuePosition].offset = leftVal;
            outputValuePosition++;
        }
    }
    return outputValuePosition;
}

static uint32_t findSmallest(const vector<overflow_value_t>& probeResults) {
    assert(probeResults.size() >= 2);
    uint32_t smallestSize = probeResults[0].numElements;
    uint32_t result = 0;
    for (auto i = 1u; i < probeResults.size(); i++) {
        if (probeResults[i].numElements < smallestSize) {
            smallestSize = probeResults[i].numElements;
            result = i;
        }
    }
    return result;
}

void Intersect::kWayIntersect() {
    auto smallestIdx = findSmallest(probedLists);
    if (smallestIdx != 0) {
        swap(probedLists[smallestIdx], probedLists[0]);
    }
    tempResultSize = 0;
    currentIdxInTempResult = 0;
    if (probedLists[0].numElements < DEFAULT_VECTOR_CAPACITY) {
        outputVector->state->selVector->selectedSize =
            twoWayIntersect(probedLists[0].value, probedLists[0].numElements, probedLists[1].value,
                probedLists[1].numElements, outputVector->values, keyLabel);
        assert(outputVector->state->selVector->selectedSize < DEFAULT_VECTOR_CAPACITY);
    } else {
        tempIntersectedResult = make_unique<nodeID_t[]>(probedLists[0].numElements);
        tempResultSize =
            twoWayIntersect(probedLists[0].value, probedLists[0].numElements, probedLists[1].value,
                probedLists[1].numElements, (uint8_t*)tempIntersectedResult.get(), keyLabel);
        auto numToCopy = min((uint32_t)DEFAULT_VECTOR_CAPACITY, tempResultSize);
        memcpy(outputVector->values, (uint8_t*)tempIntersectedResult.get(),
            numToCopy * sizeof(nodeID_t));
        currentIdxInTempResult = numToCopy;
        outputVector->state->selVector->selectedSize = numToCopy;
    }
    //    uint64_t nextToIntersect = 2;
    //    while (nextToIntersect < probedLists.size()) {
    //        outputVector->state->selVector->selectedSize = twoWayIntersect(outputVector->values,
    //            outputVector->state->selVector->selectedSize, probedLists[nextToIntersect].value,
    //            probedLists[nextToIntersect].numElements, outputVector->values, keyLabel);
    //        nextToIntersect++;
    //    }
    //    cout << "Intersect result size: " << outputVector->state->selVector->selectedSize << endl;
}

bool Intersect::getNextTuples() {
    metrics->executionTime.start();
    do {
        if (currentIdxInTempResult < tempResultSize) {
            auto numToCopy =
                min((uint32_t)DEFAULT_VECTOR_CAPACITY, tempResultSize - currentIdxInTempResult);
            memcpy(outputVector->values,
                (uint8_t*)tempIntersectedResult.get() + (currentIdxInTempResult * sizeof(nodeID_t)),
                numToCopy * sizeof(nodeID_t));
            outputVector->state->selVector->selectedSize = numToCopy;
            currentIdxInTempResult += numToCopy;
            return true;
        }
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        probe();
        kWayIntersect();
    } while (outputVector->state->selVector->selectedSize == 0);
    metrics->numOutputTuple.increase(outputVector->state->selVector->selectedSize);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
