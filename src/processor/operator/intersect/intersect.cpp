#include "include/intersect.h"

#include <algorithm>

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> Intersect::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    outKeyVector = make_shared<ValueVector>(NODE_ID);
    resultSet->dataChunks[outputDataPos.dataChunkPos]->insert(
        outputDataPos.valueVectorPos, outKeyVector);
    for (auto& dataInfo : intersectDataInfos) {
        probeKeyVectors.push_back(resultSet->getValueVector(dataInfo.keyDataPos));
        vector<uint32_t> columnIdxesToScanFrom;
        vector<shared_ptr<ValueVector>> vectorsToReadInto;
        for (auto i = 0u; i < dataInfo.payloadsDataPos.size(); i++) {
            auto payloadDataPos = dataInfo.payloadsDataPos[i];
            auto vector = make_shared<ValueVector>(dataInfo.payloadsDataType[i]);
            resultSet->dataChunks[payloadDataPos.dataChunkPos]->insert(
                payloadDataPos.valueVectorPos, vector);
            // Always skip the first two columns: build key and intersect key.
            columnIdxesToScanFrom.push_back(i + 2);
            vectorsToReadInto.push_back(vector);
        }
        payloadColumnIdxesToScanFrom.push_back(columnIdxesToScanFrom);
        payloadVectorsToScanInto.push_back(move(vectorsToReadInto));
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

void Intersect::twoWayIntersect(nodeID_t* leftNodeIDs, uint64_t leftSize,
    vector<SelectionVector*>& lSelVectors, nodeID_t* rightNodeIDs, uint64_t rightSize,
    SelectionVector* rSelVector) {
    assert(leftSize <= rightSize);
    sel_t leftPosition = 0, rightPosition = 0;
    uint64_t outputValuePosition = 0;
    while (leftPosition < leftSize && rightPosition < rightSize) {
        auto leftNodeID = leftNodeIDs[lSelVectors[0]->selectedPositions[leftPosition]];
        auto rightNodeID = rightNodeIDs[rightPosition];
        if (leftNodeID.offset < rightNodeID.offset) {
            leftPosition++;
        } else if (leftNodeID.offset > rightNodeID.offset) {
            rightPosition++;
        } else {
            for (auto selVec : lSelVectors) {
                selVec->getSelectedPositionsBuffer()[outputValuePosition] =
                    selVec->selectedPositions[leftPosition];
            }
            rSelVector->getSelectedPositionsBuffer()[outputValuePosition] = rightPosition;
            leftPosition++;
            rightPosition++;
            outputValuePosition++;
        }
    }
    // Here we need to slice all selVectors that have been previously intersected, as all lists need
    // to be selected synchronously to read payloads correctly.
    for (auto selVec : lSelVectors) {
        selVec->selectedSize = outputValuePosition;
        selVec->resetSelectorToValuePosBuffer();
    }
    rSelVector->selectedSize = outputValuePosition;
    rSelVector->resetSelectorToValuePosBuffer();
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

static vector<uint32_t> swapSmallestListToFront(vector<overflow_value_t>& lists) {
    assert(lists.size() >= 2);
    vector<uint32_t> listIdxes(lists.size());
    iota(listIdxes.begin(), listIdxes.end(), 0);
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
        swap(listIdxes[smallestListIdx], listIdxes[0]);
    }
    return listIdxes;
}

void Intersect::intersectLists(const vector<overflow_value_t>& listsToIntersect) {
    if (listsToIntersect[0].numElements == 0) {
        outKeyVector->state->selVector->selectedSize = 0;
        return;
    }
    assert(listsToIntersect[0].numElements <= DEFAULT_VECTOR_CAPACITY);
    vector<SelectionVector*> lSelVectors;
    intersectSelVectors[0]->resetSelectorToUnselected();
    // We use the first list as the anchor list to intersect with all other lists.
    auto anchorListValues = (nodeID_t*)listsToIntersect[0].value;
    for (auto i = 0u; i < listsToIntersect.size() - 1; i++) {
        lSelVectors.push_back(intersectSelVectors[i].get());
        intersectSelVectors[i + 1]->resetSelectorToUnselected();
        twoWayIntersect(anchorListValues, listsToIntersect[0].numElements, lSelVectors,
            (nodeID_t*)listsToIntersect[i + 1].value, listsToIntersect[i + 1].numElements,
            intersectSelVectors[i + 1].get());
    }
    // Populate intersect key (nodeIDs).
    auto outKeyValues = (nodeID_t*)outKeyVector->values;
    for (auto i = 0u; i < intersectSelVectors[0]->selectedSize; i++) {
        outKeyValues[i] = anchorListValues[intersectSelVectors[0]->selectedPositions[i]];
    }
    outKeyVector->state->selVector->selectedSize = intersectSelVectors[0]->selectedSize;
}

void Intersect::populatePayloads(
    const vector<uint8_t*>& tuples, const vector<uint32_t>& listIdxes) {
    for (auto i = 0u; i < listIdxes.size(); i++) {
        auto listIdx = listIdxes[i];
        auto dataInfo = intersectDataInfos[listIdx];
        sharedHTs[i]->getHashTable()->getFactorizedTable()->lookup(
            payloadVectorsToScanInto[listIdx], intersectSelVectors[i].get(),
            payloadColumnIdxesToScanFrom[listIdx], tuples[listIdx]);
    }
}

bool Intersect::getNextTuples() {
    metrics->executionTime.start();
    do {
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        auto tuples = probeHTs(getProbeKeys());
        auto listsToIntersect = fetchListsToIntersectFromTuples(tuples, isIntersectListAFlatValue);
        auto listIdxes = swapSmallestListToFront(listsToIntersect);
        intersectLists(listsToIntersect);
        if (outKeyVector->state->selVector->selectedSize != 0) {
            populatePayloads(tuples, listIdxes);
        }
    } while (outKeyVector->state->selVector->selectedSize == 0);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace kuzu
