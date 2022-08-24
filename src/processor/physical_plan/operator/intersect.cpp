#include "src/processor/include/physical_plan/operator/intersect.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

bool IntersectProbe::getChildNextTuples() {
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

void IntersectProbe::probeHT(const nodeID_t& probeKey) {
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
    probeState->probeSelVector->selectedSize = numMatchedTuples == 0 ? 0 : 1;
}

void IntersectProbe::setPrevKeyForCacheIfNecessary(const nodeID_t& probeKey) {
    if (probeState->matchedSelVector->selectedSize == 1) {
        isCachingAvailable = true;
        prevProbeKey = probeKey;
    }
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
    if (isCachingAvailable && probeKey == prevProbeKey) {
        return true;
    }
    probeHT(probeKey);
    setPrevKeyForCacheIfNecessary(probeKey);
    fetchFromHT();
    return true;
}

void IntersectProbe::fetchFromHT() {
    if (probeState->matchedSelVector->selectedSize == 0) {
        isCachingAvailable = false;
        probeResult->state->selVector->selectedSize = 0;
        return;
    }
    vector<shared_ptr<ValueVector>> vectorsToReadInto = {probeResult};
    vector<uint32_t> columnIdxsToReadFrom = {1};
    joinSharedState->getHashTable()->lookup(vectorsToReadInto, columnIdxsToReadFrom,
                                            probeState->matchedTuples.get(), probeState->nextMatchedTupleIdx, 1);
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

bool Intersect::isCurrentIntersectDone(
    const vector<shared_ptr<ValueVector>>& vectors, const vector<sel_t>& positions) {
    for (auto i = 0u; i < vectors.size(); i++) {
        if (positions[i] == vectors[i]->state->selVector->selectedSize) {
            return true;
        }
    }
    return false;
}

nodeID_t Intersect::getMaxNodeOffset(
    const vector<shared_ptr<ValueVector>>& vectors, const vector<sel_t>& positions) {
    nodeID_t currentMaxVal{0, 0};
    for (auto i = 0u; i < vectors.size(); i++) {
        auto val = ((nodeID_t*)vectors[i]->values)[positions[i]];
        if (val.offset > currentMaxVal.offset) {
            currentMaxVal = val;
        }
    }
    return currentMaxVal;
}

void Intersect::kWayIntersect() {
    vector<sel_t> inputValuePositions(2, 0);
    sel_t outputValuePosition = 0;
    auto outputValues = (nodeID_t*)outputVector->values;
    while (!isCurrentIntersectDone(probeResults, inputValuePositions)) {
        auto currentMaxNodeID = getMaxNodeOffset(probeResults, inputValuePositions);
        auto numEqualVectors = 0;
        for (auto i = 0u; i < probeResults.size(); i++) {
            auto currentValInVector = ((nodeID_t*)probeResults[i]->values)[inputValuePositions[i]];
            if (currentValInVector.offset < currentMaxNodeID.offset) {
                inputValuePositions[i]++;
            } else if (currentValInVector.offset == currentMaxNodeID.offset) {
                numEqualVectors++;
            } else {
                assert(false); // This should never happen unless vectors are not sorted.
            }
        }
        if (numEqualVectors == probeResults.size()) {
            outputValues[outputValuePosition++] = currentMaxNodeID;
            for (auto i = 0u; i < probeResults.size(); i++) {
                inputValuePositions[i]++;
            }
        }
    }
    outputVector->state->selVector->selectedSize = outputValuePosition;
}

bool Intersect::getNextTuples() {
    metrics->executionTime.start();
    do {
        if (!prober->probe()) {
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