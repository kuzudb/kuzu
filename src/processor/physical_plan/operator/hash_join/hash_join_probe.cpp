#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

#include "src/function/hash/operations/include/hash_operations.h"

using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> HashJoinProbe::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    probeState = make_unique<ProbeState>(DEFAULT_VECTOR_CAPACITY);
    probeSideKeyVector = resultSet->dataChunks[probeDataInfo.getKeyIDDataChunkPos()]
                             ->valueVectors[probeDataInfo.getKeyIDVectorPos()];
    for (auto pos : flatDataChunkPositions) {
        auto dataChunk = resultSet->dataChunks[pos];
        dataChunk->state = DataChunkState::getSingleValueDataChunkState();
    }
    for (auto i = 0u; i < probeDataInfo.nonKeyOutputDataPos.size(); ++i) {
        auto probeSideVector = make_shared<ValueVector>(
            context->memoryManager, sharedState->getNonKeyDataPosesDataTypes()[i]);
        auto [dataChunkPos, valueVectorPos] = probeDataInfo.nonKeyOutputDataPos[i];
        resultSet->dataChunks[dataChunkPos]->insert(valueVectorPos, probeSideVector);
        vectorsToRead.push_back(probeSideVector);
    }
    // We only need to read nonKeys from the factorizedTable. The first column in the
    // factorizedTable is the key column, so we skip the key column.
    columnIdxsToRead.resize(probeDataInfo.nonKeyOutputDataPos.size());
    iota(columnIdxsToRead.begin(), columnIdxsToRead.end(), 1);
    return resultSet;
}

void HashJoinProbe::getNextBatchOfMatchedTuples() {
    tuplePosToReadInProbedState = 0;
    auto factorizedTable = sharedState->getHashTable()->getFactorizedTable();
    if (factorizedTable->getNumTuples() == 0) {
        probeState->numMatchedTuples = 0;
        return;
    }
    do {
        if (probeState->numMatchedTuples == 0) {
            if (!children[0]->getNextTuples()) {
                probeState->numMatchedTuples = 0;
                return;
            }
            auto currentIdx = probeSideKeyVector->state->getPositionOfCurrIdx();
            if (probeSideKeyVector->isNull(currentIdx)) {
                continue;
            }
            probeState->probeSideKeyNodeID = ((nodeID_t*)probeSideKeyVector->values)[currentIdx];
            probeState->probedTuple =
                *sharedState->getHashTable()->findHashEntry(probeState->probeSideKeyNodeID);
        }
        probeState->numMatchedTuples = 0;
        while (probeState->probedTuple) {
            if (DEFAULT_VECTOR_CAPACITY == probeState->numMatchedTuples) {
                break;
            }
            auto nodeID = *(nodeID_t*)probeState->probedTuple;
            probeState->matchedTuples[probeState->numMatchedTuples] = probeState->probedTuple;
            probeState->numMatchedTuples += nodeID == probeState->probeSideKeyNodeID;
            probeState->probedTuple =
                *(uint8_t**)(probeState->probedTuple +
                             factorizedTable->getTableSchema()->getNullMapOffset() -
                             sizeof(uint8_t*));
        }
    } while (probeState->numMatchedTuples == 0);
}

void HashJoinProbe::populateResultSet() {
    // Copy the matched value from the build side key data chunk into the resultKeyDataChunk.
    assert(tuplePosToReadInProbedState < probeState->numMatchedTuples);
    // TODO(Xiyang/Guodong): add read multiple tuples
    auto factorizedTable = sharedState->getHashTable()->getFactorizedTable();
    factorizedTable->lookup(vectorsToRead, columnIdxsToRead, probeState->matchedTuples.get(),
        tuplePosToReadInProbedState, 1);
    tuplePosToReadInProbedState += 1;
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
        metrics->numOutputTuple.increase(probeState->numMatchedTuples);
        return true;
    }
    getNextBatchOfMatchedTuples();
    if (probeState->numMatchedTuples == 0) {
        metrics->executionTime.stop();
        return false;
    }
    populateResultSet();
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(probeState->numMatchedTuples);
    return true;
}
} // namespace processor
} // namespace graphflow
