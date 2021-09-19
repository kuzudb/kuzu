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
      probeDataChunksInfo{probeDataChunksInfo}, buildSideValueVectorsOutputPos{
                                                    move(buildSideValueVectorsOutputPos)} {}

void HashJoinProbe::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    ((HashJoinBuild&)*buildSidePrevOp).init();
    probeSideKeyDataChunk =
        this->resultSet->dataChunks[probeDataChunksInfo.keyDataPos.dataChunkPos];
    // The probe side key data chunk is also the output keyDataChunk in the resultSet. Non-key
    // vectors from the build side key data chunk are appended into the the probeSideKeyDataChunk.
    resultKeyDataChunk = probeSideKeyDataChunk;
    numProbeSidePrevKeyValueVectors = probeSideKeyDataChunk->valueVectors.size();
    probeSideKeyVector =
        probeSideKeyDataChunk->getValueVector(probeDataChunksInfo.keyDataPos.valueVectorPos);
    probeState = make_unique<ProbeState>(DEFAULT_VECTOR_CAPACITY);
    initializeResultSetAndVectorPtrs();
}

void HashJoinProbe::createVectorPtrs(DataChunk& buildSideDataChunk, uint32_t resultPos) {
    for (auto j = 0u; j < buildSideDataChunk.valueVectors.size(); j++) {
        auto vectorPtrs = make_unique<overflow_value_t[]>(DEFAULT_VECTOR_CAPACITY);
        BuildSideVectorInfo vectorInfo(buildSideDataChunk.valueVectors[j]->getNumBytesPerValue(),
            resultPos, j, buildSideVectorPtrs.size());
        buildSideVectorInfos.push_back(vectorInfo);
        buildSideVectorPtrs.push_back(move(vectorPtrs));
    }
}

void HashJoinProbe::initializeResultSetAndVectorPtrs() {
    // The buildSidePrevOp (HashJoinBuild) yields no actual resultSet, we get it from it's prevOp.
    buildSideResultSet = this->buildSidePrevOp->prevOperator->getResultSet();
    buildSideFlatResultDataChunk =
        probeDataChunksInfo.newFlatDataChunkPos == UINT32_MAX ?
            make_shared<DataChunk>(0) :
            resultSet->dataChunks[probeDataChunksInfo.newFlatDataChunkPos];
    for (auto i = 0u; i < buildSideResultSet->dataChunks.size(); i++) {
        auto buildSideDataChunk = buildSideResultSet->dataChunks[i];
        if (!buildDataChunksInfo.dataChunkPosToIsFlat[i] &&
            i != buildDataChunksInfo.keyDataPos.dataChunkPos) {
            createVectorPtrs(*buildSideDataChunk,
                probeDataChunksInfo.newUnFlatDataChunksPos[buildSideVectorPtrs.size()]);
        }
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
    do {
        if (probeState->matchedTuplesSize == 0) {
            if (!prevOperator->getNextTuples()) {
                probeState->matchedTuplesSize = 0;
                return;
            }
            probeSideKeyVector->readNodeID(
                probeSideKeyVector->state->getPositionOfCurrIdx(), probeState->probeSideKeyNodeID);
            auto directory = (uint8_t**)sharedState->htDirectory->data;
            uint64_t hash;
            Hash::operation<nodeID_t>(probeState->probeSideKeyNodeID, hash);
            hash = hash & sharedState->hashBitMask;
            probeState->probedTuple = (uint8_t*)(directory[hash]);
        }
        nodeID_t nodeIDFromHT;
        while (probeState->probedTuple) {
            if (DEFAULT_VECTOR_CAPACITY == probeState->matchedTuplesSize) {
                break;
            }
            memcpy(&nodeIDFromHT, probeState->probedTuple, NUM_BYTES_PER_NODE_ID);
            probeState->matchedTuples[probeState->matchedTuplesSize] = probeState->probedTuple;
            probeState->matchedTuplesSize +=
                (nodeIDFromHT.label == probeState->probeSideKeyNodeID.label &&
                    nodeIDFromHT.offset == probeState->probeSideKeyNodeID.offset);
            probeState->probedTuple =
                *(uint8_t**)(probeState->probedTuple + sharedState->numBytesForFixedTuplePart -
                             sizeof(uint8_t*));
        }
    } while (probeState->matchedTuplesSize == 0);
}

void HashJoinProbe::copyTuplesFromHT(DataChunk& resultDataChunk, uint64_t numResultVectors,
    uint64_t resultVectorStartPosition, uint64_t& tupleReadOffset,
    uint64_t startOffsetInResultVector, uint64_t numTuples) {
    for (auto i = 0u; i < numResultVectors; i++) {
        auto resultVector = resultDataChunk.getValueVector(resultVectorStartPosition++);
        auto numBytesPerValue = resultVector->getNumBytesPerValue();
        for (auto j = 0u; j < numTuples; j++) {
            memcpy(resultVector->values + (startOffsetInResultVector * numBytesPerValue),
                probeState->matchedTuples[j] + tupleReadOffset, numBytesPerValue);
            startOffsetInResultVector++;
        }
        tupleReadOffset += numBytesPerValue;
    }
}

void HashJoinProbe::populateResultFlatDataChunksAndVectorPtrs() {
    // Copy the matched value from the build side key data chunk into the resultKeyDataChunk.
    auto tupleReadOffset = NUM_BYTES_PER_NODE_ID;
    copyTuplesFromHT(*resultKeyDataChunk,
        buildSideResultSet->dataChunks[buildDataChunksInfo.keyDataPos.dataChunkPos]
                ->valueVectors.size() -
            1,
        numProbeSidePrevKeyValueVectors, tupleReadOffset,
        resultKeyDataChunk->state->getPositionOfCurrIdx(), 1);
    // Copy the matched values from the build side non-key data chunks.
    auto buildSideVectorPtrsPos = 0;
    auto mergedFlatDataChunkVectorPos = 0;
    for (auto i = 0u; i < buildSideResultSet->dataChunks.size(); i++) {
        if (i == buildDataChunksInfo.keyDataPos.dataChunkPos) {
            continue;
        }
        auto dataChunk = buildSideResultSet->dataChunks[i];
        if (buildDataChunksInfo.dataChunkPosToIsFlat[i]) {
            copyTuplesFromHT(*buildSideFlatResultDataChunk, dataChunk->valueVectors.size(),
                mergedFlatDataChunkVectorPos, tupleReadOffset, 0, probeState->matchedTuplesSize);
            mergedFlatDataChunkVectorPos += dataChunk->valueVectors.size();
        } else {
            for (auto& vector : dataChunk->valueVectors) {
                for (auto j = 0u; j < probeState->matchedTuplesSize; j++) {
                    buildSideVectorPtrs[buildSideVectorPtrsPos][j] =
                        *(overflow_value_t*)(probeState->matchedTuples[j] + tupleReadOffset);
                }
                tupleReadOffset += sizeof(overflow_value_t);
                buildSideVectorPtrsPos++;
            }
        }
    }
    buildSideFlatResultDataChunk->state->selectedSize = probeState->matchedTuplesSize;
    probeState->matchedTuplesSize = 0;
}

void HashJoinProbe::updateAppendedUnFlatDataChunks() {
    for (auto& buildVectorInfo : buildSideVectorInfos) {
        auto outDataChunk = resultSet->dataChunks[buildVectorInfo.resultDataChunkPos];
        auto appendVectorData =
            outDataChunk->getValueVector(buildVectorInfo.resultVectorPos)->values;
        auto overflowVal = buildSideVectorPtrs[buildVectorInfo.vectorPtrsPos].operator[](
            buildSideFlatResultDataChunk->state->currIdx);
        memcpy(appendVectorData, overflowVal.value, overflowVal.len);
        outDataChunk->state->selectedSize = overflowVal.len / buildVectorInfo.numBytesPerValue;
    }
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
    if (!buildSideVectorPtrs.empty() &&
        buildSideFlatResultDataChunk->state->currIdx <
            (int64_t)(buildSideFlatResultDataChunk->state->selectedSize - 1)) {
        buildSideFlatResultDataChunk->state->currIdx += 1;
        updateAppendedUnFlatDataChunks();
        metrics->executionTime.stop();
        metrics->numOutputTuple.increase(resultSet->getNumTuples());
        return true;
    }
    getNextBatchOfMatchedTuples();
    if (probeState->matchedTuplesSize == 0) {
        metrics->executionTime.stop();
        return false;
    }
    populateResultFlatDataChunksAndVectorPtrs();
    // UnFlat the buildSideFlatResultDataChunk if there is no build side unFlat non-key data chunks.
    buildSideFlatResultDataChunk->state->currIdx = buildSideVectorPtrs.empty() ? -1 : 0;
    updateAppendedUnFlatDataChunks();
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(resultSet->getNumTuples());
    return true;
}
} // namespace processor
} // namespace graphflow
