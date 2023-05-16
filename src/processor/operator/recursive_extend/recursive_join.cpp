#include "processor/operator/recursive_extend/recursive_join.h"

namespace kuzu {
namespace processor {

bool ScanFrontier::getNextTuplesInternal(ExecutionContext* context) {
    if (!hasExecuted) {
        hasExecuted = true;
        return true;
    }
    return false;
}

void BaseRecursiveJoin::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    for (auto& dataPos : dataInfo->vectorsToScanPos) {
        vectorsToScan.push_back(resultSet->getValueVector(dataPos).get());
    }
    srcNodeIDVector = resultSet->getValueVector(dataInfo->srcNodePos).get();
    dstNodeIDVector = resultSet->getValueVector(dataInfo->dstNodePos).get();
    if (dataInfo->trackPath) {
        pathVector = resultSet->getValueVector(dataInfo->pathPos).get();
    } else {
        pathVector = nullptr;
    }
    initLocalRecursivePlan(context);
}

bool BaseRecursiveJoin::getNextTuplesInternal(ExecutionContext* context) {
    // There are two high level steps.
    //
    // (1) BFS Computation phase: Grab a new source to do a BFS and compute an entire BFS starting
    // from a single source;
    //
    // (2) Outputting phase: Once a BFS from a single source finishes, we output the results in
    // pieces of vectors to the parent operator.
    //
    // These 2 steps are repeated iteratively until all sources to do a BFS are exhausted. The first
    // if statement checks if we are in the outputting phase and if so, scans a vector to output and
    // returns true. Otherwise, we compute a new BFS.
    while (true) {
        if (scanOutput()) { // Phase 2
            return true;
        }
        auto inputFTableMorsel = sharedState->inputFTableSharedState->getMorsel(1 /* morselSize */);
        if (inputFTableMorsel->numTuples == 0) { // All src have been exhausted.
            return false;
        }
        sharedState->inputFTableSharedState->getTable()->scan(vectorsToScan,
            inputFTableMorsel->startTupleIdx, inputFTableMorsel->numTuples,
            dataInfo->colIndicesToScan);
        bfsMorsel->resetState();
        computeBFS(context); // Phase 1
        frontiersScanner->resetState(*bfsMorsel);
    }
}

bool BaseRecursiveJoin::scanOutput() {
    common::sel_t offsetVectorSize = 0u;
    common::sel_t dataVectorSize = 0u;
    if (pathVector != nullptr) {
        common::ListVector::resetListAuxiliaryBuffer(pathVector);
    }
    frontiersScanner->scan(
        nodeTable->getTableID(), pathVector, dstNodeIDVector, offsetVectorSize, dataVectorSize);
    if (offsetVectorSize == 0) {
        return false;
    }
    dstNodeIDVector->state->initOriginalAndSelectedSize(offsetVectorSize);
    return true;
}

void BaseRecursiveJoin::computeBFS(ExecutionContext* context) {
    auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
        srcNodeIDVector->state->selVector->selectedPositions[0]);
    bfsMorsel->markSrc(nodeID.offset);
    while (!bfsMorsel->isComplete()) {
        auto boundNodeOffset = bfsMorsel->getNextNodeOffset();
        if (boundNodeOffset != common::INVALID_OFFSET) {
            // Found a starting node from current frontier.
            scanBFSLevel->setNodeID(common::nodeID_t{boundNodeOffset, nodeTable->getTableID()});
            while (recursiveRoot->getNextTuple(context)) { // Exhaust recursive plan.
                updateVisitedNodes(boundNodeOffset);
            }
        } else {
            // Otherwise move to the next frontier.
            bfsMorsel->finalizeCurrentLevel();
        }
    }
}

void BaseRecursiveJoin::updateVisitedNodes(common::offset_t boundNodeOffset) {
    auto boundNodeMultiplicity = bfsMorsel->currentFrontier->getMultiplicity(boundNodeOffset);
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        bfsMorsel->markVisited(boundNodeOffset, nodeID.offset, boundNodeMultiplicity);
    }
}

// ResultSet for list extend, i.e. 2 data chunks each with 1 vector.
static std::unique_ptr<ResultSet> populateResultSetWithTwoDataChunks() {
    auto resultSet = std::make_unique<ResultSet>(2);
    auto dataChunk0 = std::make_shared<common::DataChunk>(1);
    dataChunk0->state = common::DataChunkState::getSingleValueDataChunkState();
    dataChunk0->insert(
        0, std::make_shared<common::ValueVector>(common::LogicalTypeID::INTERNAL_ID, nullptr));
    auto dataChunk1 = std::make_shared<common::DataChunk>(1);
    dataChunk1->insert(
        0, std::make_shared<common::ValueVector>(common::LogicalTypeID::INTERNAL_ID, nullptr));
    resultSet->insert(0, std::move(dataChunk0));
    resultSet->insert(1, std::move(dataChunk1));
    return resultSet;
}

// ResultSet for column extend, i.e. 1 data chunk with 2 vectors.
static std::unique_ptr<ResultSet> populateResultSetWithOneDataChunk() {
    auto resultSet = std::make_unique<ResultSet>(1);
    auto dataChunk0 = std::make_shared<common::DataChunk>(2);
    dataChunk0->state = common::DataChunkState::getSingleValueDataChunkState();
    dataChunk0->insert(
        0, std::make_shared<common::ValueVector>(common::LogicalTypeID::INTERNAL_ID, nullptr));
    dataChunk0->insert(
        1, std::make_shared<common::ValueVector>(common::LogicalTypeID::INTERNAL_ID, nullptr));
    resultSet->insert(0, std::move(dataChunk0));
    return resultSet;
}

std::unique_ptr<ResultSet> BaseRecursiveJoin::getLocalResultSet() {
    auto numDataChunks = dataInfo->tmpDstNodePos.dataChunkPos + 1;
    if (numDataChunks == 2) {
        return populateResultSetWithTwoDataChunks();
    } else {
        assert(dataInfo->tmpDstNodePos.dataChunkPos == 0);
        return populateResultSetWithOneDataChunk();
    }
}

void BaseRecursiveJoin::initLocalRecursivePlan(ExecutionContext* context) {
    auto op = recursiveRoot.get();
    while (!op->isSource()) {
        assert(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    scanBFSLevel = (ScanFrontier*)op;
    localResultSet = getLocalResultSet();
    tmpDstNodeIDVector = localResultSet->getValueVector(dataInfo->tmpDstNodePos).get();
    recursiveRoot->initLocalState(localResultSet.get(), context);
}

} // namespace processor
} // namespace kuzu
