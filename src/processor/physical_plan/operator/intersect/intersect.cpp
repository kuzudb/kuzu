#include "src/processor/include/physical_plan/operator/intersect/intersect.h"

#include <algorithm>

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Intersect::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    outputVector = make_shared<ValueVector>(NODE_ID);
    resultSet->dataChunks[outputDataPos.dataChunkPos]->insert(
        outputDataPos.valueVectorPos, outputVector);
    probeKeyVectors.resize(probeKeysDataPos.size());
    for (auto i = 0u; i < probeKeysDataPos.size(); i++) {
        probeKeyVectors[i] = resultSet->getValueVector(probeKeysDataPos[i]);
    }
    for (auto& sharedHT : sharedHTs) {
        isIntersectListAFlatValue.push_back(
            sharedHT->getHashTable()->getTableSchema()->getColumn(1)->isFlat());
    }
    return resultSet;
}

vector<uint8_t*> Intersect::probeHTs(const vector<nodeID_t>& keys) {
    vector<uint8_t*> tuples(keys.size());
    hash_t tmpHash;
    for (auto i = 0u; i < keys.size(); i++) {
        Hash::operation<nodeID_t>(keys[i], false, tmpHash);
        tuples[i] = sharedHTs[i]->getHashTable()->getTupleForHash(tmpHash);
        while (tuples[i]) {
            if (*(nodeID_t*)tuples[i] == keys[i]) {
                break;
            }
            tuples[i] = *sharedHTs[i]->getHashTable()->getPrevTuple(tuples[i]);
        }
    }
    return tuples;
}

uint64_t Intersect::twoWayIntersect(nodeID_t* leftNodeIDs, uint64_t leftSize,
    nodeID_t* rightNodeIDs, uint64_t rightSize, nodeID_t* outputNodeIDs) {
    assert(leftSize <= rightSize);
    sel_t leftPosition = 0, rightPosition = 0;
    uint64_t outputValuePosition = 0;
    while (leftPosition < leftSize && rightPosition < rightSize) {
        auto leftNodeID = leftNodeIDs[leftPosition];
        auto rightNodeID = rightNodeIDs[rightPosition];
        if (leftNodeID.offset < rightNodeID.offset) {
            leftPosition++;
        } else if (leftNodeID.offset > rightNodeID.offset) {
            rightPosition++;
        } else {
            leftPosition++;
            rightPosition++;
            outputNodeIDs[outputValuePosition] = leftNodeID;
            outputValuePosition++;
        }
    }
    return outputValuePosition;
}

vector<nodeID_t> Intersect::getProbeKeys() {
    vector<nodeID_t> keys(probeKeyVectors.size());
    for (auto i = 0u; i < keys.size(); i++) {
        assert(probeKeyVectors[i]->state->isFlat());
        keys[i] = ((nodeID_t*)probeKeyVectors[i]
                       ->values)[probeKeyVectors[i]->state->getPositionOfCurrIdx()];
    }
    return keys;
}

static vector<overflow_value_t> fetchListsToIntersectFromTuples(
    const vector<uint8_t*>& tuples, const vector<bool>& isFlatValue) {
    vector<overflow_value_t> listsToIntersect(tuples.size());
    for (auto i = 0u; i < tuples.size(); i++) {
        if (tuples[i]) {
            if (isFlatValue[i]) {
                listsToIntersect[i].numElements = 1;
                listsToIntersect[i].value = tuples[i] + sizeof(nodeID_t);
            } else {
                // The list to intersect is placed right after the key in tuple.
                listsToIntersect[i] = *(overflow_value_t*)(tuples[i] + sizeof(nodeID_t));
            }
        }
    }
    return listsToIntersect;
}

static void swapSmallestListToFront(vector<overflow_value_t>& lists) {
    assert(lists.size() >= 2);
    uint32_t smallestListSize = lists[0].numElements;
    uint32_t smallestListIdx = 0;
    for (auto i = 1u; i < lists.size(); i++) {
        if (lists[i].numElements < smallestListSize) {
            smallestListSize = lists[i].numElements;
            smallestListIdx = i;
        }
    }
    if (smallestListIdx != 0) {
        swap(lists[smallestListIdx], lists[0]);
    }
}

void Intersect::intersectLists() {
    auto tuples = probeHTs(getProbeKeys());
    auto listsToIntersect = fetchListsToIntersectFromTuples(tuples, isIntersectListAFlatValue);
    swapSmallestListToFront(listsToIntersect);
    if (listsToIntersect[0].numElements == 0) {
        outputVector->state->selVector->selectedSize = 0;
        return;
    }
    assert(listsToIntersect[0].numElements <= DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < listsToIntersect.size() - 1; i++) {
        auto leftListValues = i == 0 ? listsToIntersect[0].value : outputVector->values;
        auto leftListSize =
            i == 0 ? listsToIntersect[0].numElements : outputVector->state->selVector->selectedSize;
        outputVector->state->selVector->selectedSize = twoWayIntersect((nodeID_t*)leftListValues,
            leftListSize, (nodeID_t*)listsToIntersect[i + 1].value,
            listsToIntersect[i + 1].numElements, (nodeID_t*)outputVector->values);
    }
}

bool Intersect::getNextTuples() {
    metrics->executionTime.start();
    do {
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        intersectLists();
    } while (outputVector->state->selVector->selectedSize == 0);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
