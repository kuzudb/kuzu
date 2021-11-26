#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

#include "src/common/include/operations/hash_operations.h"

namespace graphflow {
namespace processor {

HashJoinProbe::HashJoinProbe(const BuildDataChunksInfo& buildDataChunksInfo,
    const ProbeDataChunksInfo& probeDataChunksInfo,
    vector<unordered_map<uint32_t, DataPos>> buildSideValueVectorsOutputPos,
    unique_ptr<PhysicalOperator> buildSidePrevOp, unique_ptr<PhysicalOperator> probeSidePrevOp,
    ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(probeSidePrevOp), HASH_JOIN_PROBE, context, id},
      buildSidePrevOp{move(buildSidePrevOp)}, buildDataChunksInfo{buildDataChunksInfo},
      probeDataChunksInfo{probeDataChunksInfo}, buildSideValueVectorsOutputPos{move(
                                                    buildSideValueVectorsOutputPos)},
      tuplePosToReadInProbedState{0} {}

void HashJoinProbe::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    ((HashJoinBuild&)*buildSidePrevOp).init();
    probeState = make_unique<ProbeState>(DEFAULT_VECTOR_CAPACITY);
    constructResultVectorsPosAndFieldsToRead();
    initializeResultSet();
}

void HashJoinProbe::constructResultVectorsPosAndFieldsToRead() {
    auto& keyDataChunkOutputPosMap =
        buildSideValueVectorsOutputPos[buildDataChunksInfo.keyDataPos.dataChunkPos];
    auto fieldId = 1; // SKip the first key field.
    for (auto i = 0u; i < keyDataChunkOutputPosMap.size(); i++) {
        if (i == buildDataChunksInfo.keyDataPos.valueVectorPos) {
            continue;
        }
        resultVectorsPos.push_back(keyDataChunkOutputPosMap.at(i));
        fieldsToRead.push_back(fieldId++);
    }
    for (auto i = 0u; i < buildDataChunksInfo.dataChunkPosToIsFlat.size(); i++) {
        if (i == buildDataChunksInfo.keyDataPos.dataChunkPos) {
            continue;
        }
        auto& dataChunkOutputPos = buildSideValueVectorsOutputPos[i];
        for (auto j = 0u; j < dataChunkOutputPos.size(); j++) {
            resultVectorsPos.push_back(dataChunkOutputPos.at(j));
            fieldsToRead.push_back(fieldId++);
        }
    }
}

void HashJoinProbe::initializeResultSet() {
    // The buildSidePrevOp (HashJoinBuild) yields no actual resultSet, we get it from it's prevOp.
    buildSideResultSet = this->buildSidePrevOp->prevOperator->getResultSet();
    probeSideKeyVector = resultSet->dataChunks[probeDataChunksInfo.keyDataPos.dataChunkPos]
                             ->valueVectors[probeDataChunksInfo.keyDataPos.valueVectorPos];
    for (auto i = 0u; i < buildSideResultSet->dataChunks.size(); i++) {
        auto buildSideDataChunk = buildSideResultSet->dataChunks[i];
        for (auto [buildSideValueVectorPos, outputPos] : buildSideValueVectorsOutputPos[i]) {
            auto buildSideValueVector = buildSideDataChunk->valueVectors[buildSideValueVectorPos];
            auto probeSideValueVector =
                make_shared<ValueVector>(context.memoryManager, buildSideValueVector->dataType);
            auto [outDataChunkPos, outValueVectorPos] = outputPos;
            resultSet->dataChunks[outDataChunkPos]->insert(outValueVectorPos, probeSideValueVector);
        }
    }
}

void HashJoinProbe::getNextBatchOfMatchedTuples() {
    probeState->numMatchedTuples = 0;
    tuplePosToReadInProbedState = 0;
    do {
        if (probeState->numMatchedTuples == 0) {
            if (!prevOperator->getNextTuples()) {
                probeState->numMatchedTuples = 0;
                return;
            }
            probeSideKeyVector->readNodeID(
                probeSideKeyVector->state->getPositionOfCurrIdx(), probeState->probeSideKeyNodeID);
            auto directory = (uint8_t**)sharedState->htDirectory->data;
            hash_t hash;
            Hash::operation<nodeID_t>(probeState->probeSideKeyNodeID, false /* isNull */, hash);
            hash = hash & sharedState->hashBitMask;
            probeState->probedTuple = (uint8_t*)(directory[hash]);
        }
        while (probeState->probedTuple) {
            if (DEFAULT_VECTOR_CAPACITY == probeState->numMatchedTuples) {
                break;
            }
            auto nodeID = *(nodeID_t*)probeState->probedTuple;
            probeState->matchedTuples[probeState->numMatchedTuples] = probeState->probedTuple;
            probeState->numMatchedTuples += nodeID == probeState->probeSideKeyNodeID;
            probeState->probedTuple =
                *(uint8_t**)(probeState->probedTuple +
                             sharedState->rowCollection->getLayout().numBytesPerRow -
                             sizeof(uint8_t*));
        }
    } while (probeState->numMatchedTuples == 0);
}

void HashJoinProbe::populateResultSet() {
    // Copy the matched value from the build side key data chunk into the resultKeyDataChunk.
    assert(tuplePosToReadInProbedState < probeState->numMatchedTuples);
    tuplePosToReadInProbedState += sharedState->rowCollection->lookup(fieldsToRead,
        resultVectorsPos, *resultSet, probeState->matchedTuples.get(), tuplePosToReadInProbedState,
        probeState->numMatchedTuples - tuplePosToReadInProbedState);
}

void HashJoinProbe::reInitialize() {
    PhysicalOperator::reInitialize();
    buildSidePrevOp->reInitialize();
}

// The general flow of a hash join probe:
// 1) find matched tuples of probe side key from ht.
// 2) populate values from matched tuples into resultKeyDataChunk , buildSideFlatResultDataChunk
// (all flat data chunks from the build side are merged into one) and buildSideVectorPtrs (each
// VectorPtr corresponds to one unFlat build side data chunk that is appended to the resultSet).
// 3) flat buildSideFlatResultDataChunk, updating its currIdx, and also populates appended unFlat
// data chunks. If there is no appended unFlat data chunks, which means buildSideVectorPtrs is
// empty, directly unFlat buildSideFlatResultDataChunk (if it exists).
bool HashJoinProbe::getNextTuples() {
    metrics->executionTime.start();
    if (tuplePosToReadInProbedState < probeState->numMatchedTuples) {
        populateResultSet();
        metrics->executionTime.stop();
        metrics->numOutputTuple.increase(resultSet->getNumTuples());
        return true;
    }
    getNextBatchOfMatchedTuples();
    if (probeState->numMatchedTuples == 0) {
        metrics->executionTime.stop();
        return false;
    }
    populateResultSet();
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(resultSet->getNumTuples());
    return true;
}
} // namespace processor
} // namespace graphflow
