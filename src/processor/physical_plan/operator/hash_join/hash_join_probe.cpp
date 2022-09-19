#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

#include "src/function/hash/include/vector_hash_operations.h"
#include "src/function/hash/operations/include/hash_operations.h"

using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> HashJoinProbe::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    probeState = make_unique<ProbeState>();
    probeKeyVector = resultSet->dataChunks[probeDataInfo.getKeyIDDataChunkPos()]
                         ->valueVectors[probeDataInfo.getKeyIDVectorPos()];
    for (auto pos : flatDataChunkPositions) {
        auto dataChunk = resultSet->dataChunks[pos];
        dataChunk->state = DataChunkState::getSingleValueDataChunkState();
    }
    for (auto i = 0u; i < probeDataInfo.nonKeyOutputDataPos.size(); ++i) {
        auto probeSideNonKeyVector = make_shared<ValueVector>(
            sharedState->getNonKeyDataPosesDataTypes()[i], context->memoryManager);
        auto [dataChunkPos, valueVectorPos] = probeDataInfo.nonKeyOutputDataPos[i];
        resultSet->dataChunks[dataChunkPos]->insert(valueVectorPos, probeSideNonKeyVector);
        vectorsToReadInto.push_back(probeSideNonKeyVector);
    }
    // We only need to read nonKeys from the factorizedTable. The first column in the
    // factorizedTable is the key column, so we skip the key column.
    columnIdxsToReadFrom.resize(probeDataInfo.nonKeyOutputDataPos.size());
    iota(columnIdxsToReadFrom.begin(), columnIdxsToReadFrom.end(), 1);
    return resultSet;
}

bool HashJoinProbe::hasMoreLeft() {
    if (probeKeyVector->state->isFlat() &&
        probeState->probedTuples[probeKeyVector->state->getPositionOfCurrIdx()] != nullptr) {
        return true;
    }
    return false;
}

bool HashJoinProbe::getNextBatchOfMatchedTuples() {
    if (probeState->nextMatchedTupleIdx < probeState->matchedSelVector->selectedSize) {
        return true;
    }
    if (!hasMoreLeft()) {
        restoreSelVector(probeKeyVector->state->selVector.get());
        if (!children[0]->getNextTuples()) {
            return false;
        }
        saveSelVector(probeKeyVector->state->selVector.get());
        sharedState->getHashTable()->probe(*probeKeyVector, probeState->probedTuples.get());
    }
    auto keys = (nodeID_t*)probeKeyVector->values;
    auto numMatchedTuples = 0;
    auto startKeyIdx = probeKeyVector->state->isFlat() ? probeKeyVector->state->currIdx : 0;
    auto numKeys =
        probeKeyVector->state->isFlat() ? 1 : probeKeyVector->state->selVector->selectedSize;
    for (auto i = 0u; i < numKeys; i++) {
        auto pos = probeKeyVector->state->selVector->selectedPositions[i + startKeyIdx];
        while (probeState->probedTuples[pos]) {
            if (numMatchedTuples == DEFAULT_VECTOR_CAPACITY) {
                break;
            }
            auto currentTuple = probeState->probedTuples[pos];
            probeState->matchedTuples[numMatchedTuples] = currentTuple;
            probeState->matchedSelVector->selectedPositions[numMatchedTuples] = pos;
            numMatchedTuples += *(nodeID_t*)currentTuple == keys[pos];
            probeState->probedTuples[pos] =
                *sharedState->getHashTable()->getPrevTuple(currentTuple);
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
            assert(vector->state != probeKeyVector->state);
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
        probeKeyVector->state->isFlat() ? 1 : probeState->matchedSelVector->selectedSize;
    if (!probeKeyVector->state->isFlat() &&
        probeKeyVector->state->selVector->selectedSize != numTuplesToRead) {
        // Update probeSideKeyVector's selectedPositions when the probe side is unflat and its
        // selected positions need to change (i.e., some keys has no matched tuples).
        auto keySelectedBuffer = probeKeyVector->state->selVector->getSelectedPositionsBuffer();
        for (auto i = 0u; i < numTuplesToRead; i++) {
            keySelectedBuffer[i] = probeState->matchedSelVector->selectedPositions[i];
        }
        probeKeyVector->state->selVector->selectedSize = numTuplesToRead;
        probeKeyVector->state->selVector->resetSelectorToValuePosBuffer();
    }
    sharedState->getHashTable()->lookup(vectorsToReadInto, columnIdxsToReadFrom,
        probeState->matchedTuples.get(), probeState->nextMatchedTupleIdx, numTuplesToRead);
    probeState->nextMatchedTupleIdx += numTuplesToRead;
    return numTuplesToRead;
}

uint64_t HashJoinProbe::getNextLeftJoinResult() {
    assert(probeKeyVector->state->isFlat());
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
