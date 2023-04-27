#include "processor/operator/var_length_extend/recursive_join.h"

namespace kuzu {
namespace processor {

bool ScanFrontier::getNextTuplesInternal(ExecutionContext* context) {
    if (!hasExecuted) {
        hasExecuted = true;
        return true;
    }
    return false;
}

void RecursiveJoin::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    maxNodeOffset = nodeTable->getMaxNodeOffset(transaction);
    for (auto& dataPos : vectorsToScanPos) {
        vectorsToScan.push_back(resultSet->getValueVector(dataPos).get());
    }
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDVectorPos);
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDVectorPos);
    distanceVector = resultSet->getValueVector(distanceVectorPos);
    bfsMorsel = std::make_unique<BFSMorsel>(maxNodeOffset, upperBound);
    initLocalRecursivePlan(context);
    bfsScanState.resetState();
}

// There are two high level steps.
//
// (1) BFS Computation phase: Grab a new source to do a BFS and compute an entire BFS starting from
// a single source;
//
// (2) Outputting phase: Once a computation finishes, we output the results in pieces of vectors to
// the parent operator.
//
// These 2 steps are repeated iteratively until all sources to do a recursive computation are
// exhausted. The first if statement checks if we are in the outputting phase and if so, scans a
// vector to output and returns true. Otherwise, we compute a new BFS.
bool RecursiveJoin::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (bfsScanState.numScanned < bfsMorsel->numVisitedNodes) { // Phase 2
            auto numToScan = std::min<uint64_t>(common::DEFAULT_VECTOR_CAPACITY,
                bfsMorsel->numVisitedNodes - bfsScanState.numScanned);
            scanDstNodes(numToScan);
            bfsScanState.numScanned += numToScan;
            return true;
        }
        if (!computeBFS(context)) { // Phase 1
            return false;
        }
    }
}

bool RecursiveJoin::computeBFS(ExecutionContext* context) {
    auto inputFTableMorsel = inputFTableSharedState->getMorsel(1);
    if (inputFTableMorsel->numTuples == 0) {
        return false;
    }
    inputFTableSharedState->getTable()->scan(vectorsToScan, inputFTableMorsel->startTupleIdx,
        inputFTableMorsel->numTuples, colIndicesToScan);
    bfsMorsel->resetState();
    auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
        srcNodeIDVector->state->selVector->selectedPositions[0]);
    bfsMorsel->markSrc(nodeID.offset);
    while (!bfsMorsel->isComplete()) {
        auto nodeOffset = bfsMorsel->getNextNodeOffset();
        if (nodeOffset != common::INVALID_NODE_OFFSET) {
            // Found a starting node from current frontier.
            scanBFSLevel->setNodeID(common::nodeID_t{nodeOffset, nodeTable->getTableID()});
            while (root->getNextTuple(context)) { // Exhaust recursive plan.
                updateVisitedState();
            }
        } else {
            // Otherwise move to the next frontier.
            bfsMorsel->moveNextLevelAsCurrentLevel();
        }
    }
    bfsMorsel->unmarkSrc();
    bfsScanState.resetState();
    return true;
}

void RecursiveJoin::updateVisitedState() {
    auto visitedNodes = bfsMorsel->visitedNodes;
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        if (visitedNodes[nodeID.offset] == VisitedState::NOT_VISITED) {
            bfsMorsel->markVisited(nodeID.offset);
        }
    }
}

std::unique_ptr<ResultSet> RecursiveJoin::getLocalResultSet() {
    // Create two datachunks each with 1 nodeID value vector.
    // DataChunk 1 has flat state and contains the temporary src node ID vector for recursive join.
    // DataChunk 2 has unFlat state and contains the temporary dst node ID vector for recursive
    // join.
    auto resultSet = std::make_unique<ResultSet>(2);
    auto dataChunk0 = std::make_shared<common::DataChunk>(1);
    dataChunk0->state = common::DataChunkState::getSingleValueDataChunkState();
    dataChunk0->insert(0, std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr));
    auto dataChunk1 = std::make_shared<common::DataChunk>(1);
    dataChunk1->insert(0, std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr));
    resultSet->insert(0, std::move(dataChunk0));
    resultSet->insert(1, std::move(dataChunk1));
    return resultSet;
}

void RecursiveJoin::initLocalRecursivePlan(ExecutionContext* context) {
    auto op = root.get();
    while (!op->isSource()) {
        assert(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    scanBFSLevel = (ScanFrontier*)op;
    localResultSet = getLocalResultSet();
    tmpDstNodeIDVector = localResultSet->getValueVector(getTmpDstNodeVectorPos());
    root->initLocalState(localResultSet.get(), context);
}

void RecursiveJoin::scanDstNodes(size_t sizeToScan) {
    auto size = 0;
    while (sizeToScan != size) {
        if (bfsMorsel->visitedNodes[bfsScanState.currentOffset] == NOT_VISITED) {
            bfsScanState.currentOffset++;
            continue;
        }
        dstNodeIDVector->setValue<common::nodeID_t>(
            size, common::nodeID_t{bfsScanState.currentOffset, nodeTable->getTableID()});
        distanceVector->setValue<int64_t>(size, bfsMorsel->distance[bfsScanState.currentOffset]);
        size++;
        bfsScanState.currentOffset++;
    }
    dstNodeIDVector->state->initOriginalAndSelectedSize(sizeToScan);
}

} // namespace processor
} // namespace kuzu
