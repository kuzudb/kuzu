#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

#include "src/function/hash/include/vector_hash_operations.h"
#include "src/function/hash/operations/include/hash_operations.h"

using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> HashJoinProbe::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    probeState = make_unique<ProbeState>();
    for (auto& keyDataPos : probeDataInfo.keysDataPos) {
        auto keyVector =
            resultSet->dataChunks[keyDataPos.dataChunkPos]->valueVectors[keyDataPos.valueVectorPos];
        keyVectors.push_back(keyVector);
        keySelVectors.push_back(keyVector->state->selVector.get());
    }
    for (auto pos : flatDataChunkPositions) {
        auto dataChunk = resultSet->dataChunks[pos];
        dataChunk->state = DataChunkState::getSingleValueDataChunkState();
    }
    for (auto i = 0u; i < probeDataInfo.payloadsOutputDataPos.size(); ++i) {
        auto probePayloadVector =
            make_shared<ValueVector>(sharedState->getPayloadDataTypes()[i], context->memoryManager);
        auto [dataChunkPos, valueVectorPos] = probeDataInfo.payloadsOutputDataPos[i];
        resultSet->dataChunks[dataChunkPos]->insert(valueVectorPos, probePayloadVector);
        vectorsToReadInto.push_back(probePayloadVector);
    }
    // We only need to read nonKeys from the factorizedTable. The first column in the
    // factorizedTable is the key column, so we skip the key column.
    columnIdxsToReadFrom.resize(probeDataInfo.payloadsOutputDataPos.size());
    iota(columnIdxsToReadFrom.begin(), columnIdxsToReadFrom.end(), 1);
    return resultSet;
}

bool HashJoinProbe::hasMoreLeft() {
    if (keyVectors[0]->state->isFlat() && probeState->probedTuples[0] != nullptr) {
        return true;
    }
    return false;
}

bool HashJoinProbe::getNextBatchOfMatchedTuples() {
    if (probeState->nextMatchedTupleIdx < probeState->matchedSelVector->selectedSize) {
        return true;
    }
    if (!hasMoreLeft()) {
        restoreSelVectors(keySelVectors);
        if (!children[0]->getNextTuples()) {
            return false;
        }
        saveSelVectors(keySelVectors);
        sharedState->getHashTable()->probe(keyVectors, probeState->probedTuples.get());
    }
    auto numMatchedTuples = 0;
    if (keyVectors[0]->state->isFlat()) {
        // probe side is flat.
        while (probeState->probedTuples[0]) {
            if (numMatchedTuples == DEFAULT_VECTOR_CAPACITY) {
                break;
            }
            auto currentTuple = probeState->probedTuples[0];
            probeState->matchedTuples[numMatchedTuples] = currentTuple;
            bool isKeysEqual = true;
            for (auto i = 0u; i < keyVectors.size(); i++) {
                auto keyValues = (nodeID_t*)keyVectors[i]->values;
                if (((nodeID_t*)currentTuple)[i] !=
                    keyValues[keyVectors[i]->state->getPositionOfCurrIdx()]) {
                    isKeysEqual = false;
                    break;
                }
            }
            numMatchedTuples += isKeysEqual;
            probeState->probedTuples[0] = *sharedState->getHashTable()->getPrevTuple(currentTuple);
        }
    } else {
        assert(keyVectors.size() == 1);
        auto keyValues = (nodeID_t*)keyVectors[0]->values;
        for (auto i = 0u; i < keyVectors[0]->state->selVector->selectedSize; i++) {
            auto pos = keyVectors[0]->state->selVector->selectedPositions[i];
            while (probeState->probedTuples[i]) {
                assert(numMatchedTuples <= DEFAULT_VECTOR_CAPACITY);
                auto currentTuple = probeState->probedTuples[i];
                probeState->matchedTuples[numMatchedTuples] = currentTuple;
                probeState->matchedSelVector->selectedPositions[numMatchedTuples] = pos;
                numMatchedTuples += *(nodeID_t*)currentTuple == keyValues[pos];
                probeState->probedTuples[i] =
                    *sharedState->getHashTable()->getPrevTuple(currentTuple);
            }
        }
    }
    probeState->matchedSelVector->selectedSize = numMatchedTuples;
    probeState->nextMatchedTupleIdx = 0;
    return true;
}

void HashJoinProbe::setVectorsToNull(vector<shared_ptr<ValueVector>>& vectors) {
    for (auto& vector : vectorsToReadInto) {
        if (vector->state->isFlat()) {
            vector->setNull(vector->state->getPositionOfCurrIdx(), true);
        } else {
            assert(vector->state != keyVectors[0]->state);
            auto pos = vector->state->selVector->selectedPositions[0];
            vector->setNull(pos, true);
            vector->state->selVector->selectedSize = 1;
        }
    }
}

uint64_t HashJoinProbe::getNextInnerJoinResult() {
    if (probeState->matchedSelVector->selectedSize == 0) {
        return 0;
    }
    auto numTuplesToRead =
        keyVectors[0]->state->isFlat() ? 1 : probeState->matchedSelVector->selectedSize;
    if (!keyVectors[0]->state->isFlat() &&
        keyVectors[0]->state->selVector->selectedSize != numTuplesToRead) {
        // Update probeSideKeyVector's selectedPositions when the probe side is unflat and its
        // selected positions need to change (i.e., some keys has no matched tuples).
        auto keySelectedBuffer = keyVectors[0]->state->selVector->getSelectedPositionsBuffer();
        for (auto i = 0u; i < numTuplesToRead; i++) {
            keySelectedBuffer[i] = probeState->matchedSelVector->selectedPositions[i];
        }
        keyVectors[0]->state->selVector->selectedSize = numTuplesToRead;
        keyVectors[0]->state->selVector->resetSelectorToValuePosBuffer();
    }
    sharedState->getHashTable()->lookup(vectorsToReadInto, columnIdxsToReadFrom,
        probeState->matchedTuples.get(), probeState->nextMatchedTupleIdx, numTuplesToRead);
    probeState->nextMatchedTupleIdx += numTuplesToRead;
    return numTuplesToRead;
}

uint64_t HashJoinProbe::getNextLeftJoinResult() {
    if (getNextInnerJoinResult() == 0) {
        setVectorsToNull(vectorsToReadInto);
    }
    return 1;
}

// The general flow of a hash join probe:
// 1) find matched tuples of probe side key from ht.
// 2) populate values from matched tuples into resultKeyDataChunk , buildSideFlatResultDataChunk
// (all flat data chunks from the build side are merged into one) and buildSideVectorPtrs (each
// VectorPtr corresponds to one unFlat build side data chunk that is appended to the resultSet).
bool HashJoinProbe::getNextTuples() {
    metrics->executionTime.start();
    uint64_t numPopulatedTuples;
    do {
        if (!getNextBatchOfMatchedTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        numPopulatedTuples = getNextJoinResult();
    } while (numPopulatedTuples == 0);
    metrics->numOutputTuple.increase(numPopulatedTuples);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
