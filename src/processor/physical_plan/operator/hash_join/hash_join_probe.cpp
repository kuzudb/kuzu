#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

#include "src/common/include/operations/hash_operations.h"

namespace graphflow {
namespace processor {

HashJoinProbe::HashJoinProbe(const BuildDataInfo& buildDataInfo, const ProbeDataInfo& probeDataInfo,
    unique_ptr<PhysicalOperator> buildSidePrevOp, unique_ptr<PhysicalOperator> probeSidePrevOp,
    ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(probeSidePrevOp), HASH_JOIN_PROBE, context, id}, buildSidePrevOp{move(
                                                                                 buildSidePrevOp)},
      buildDataInfo{buildDataInfo}, probeDataInfo{probeDataInfo}, tuplePosToReadInProbedState{0} {}

void HashJoinProbe::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    ((HashJoinBuild&)*buildSidePrevOp).init();
    probeState = make_unique<ProbeState>(DEFAULT_VECTOR_CAPACITY);
    constructResultVectorsPosAndFieldsToRead();
    initializeResultSet();
}

void HashJoinProbe::constructResultVectorsPosAndFieldsToRead() {
    // Skip the first key field.
    auto fieldId = 1;
    for (auto& nonKeyDataPos : probeDataInfo.nonKeyDataPoses) {
        resultVectorsPos.push_back(nonKeyDataPos);
        fieldsToRead.push_back(fieldId++);
    }
}

void HashJoinProbe::initializeResultSet() {
    // The buildSidePrevOp (HashJoinBuild) yields no actual resultSet, we get it from it's prevOp.
    buildSideResultSet = this->buildSidePrevOp->prevOperator->getResultSet();
    probeSideKeyVector = resultSet->dataChunks[probeDataInfo.getKeyIDDataChunkPos()]
                             ->valueVectors[probeDataInfo.getKeyIDVectorPos()];
    assert(buildDataInfo.nonKeyDataPoses.size() == probeDataInfo.nonKeyDataPoses.size());
    for (auto i = 0u; i < buildDataInfo.nonKeyDataPoses.size(); ++i) {
        auto [buildSideDataChunkPos, buildSideVectorPos] = buildDataInfo.nonKeyDataPoses[i];
        auto buildSideDataChunk = buildSideResultSet->dataChunks[buildSideDataChunkPos];
        auto buildSideVector = buildSideDataChunk->valueVectors[buildSideVectorPos];
        auto [probeSideDataChunkPos, probeSideVectorPos] = probeDataInfo.nonKeyDataPoses[i];
        auto probeSideVector =
            make_shared<ValueVector>(context.memoryManager, buildSideVector->dataType);
        this->resultSet->dataChunks[probeSideDataChunkPos]->insert(
            probeSideVectorPos, probeSideVector);
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
