#include "processor/operator/var_length_extend/recursive_join.h"

namespace kuzu {
namespace processor {

bool ScanBFSLevel::getNextTuplesInternal(ExecutionContext* context) {
    if (!hasExecuted) {
        hasExecuted = true;
        return true;
    }
    return false;
}

void RecursiveJoin::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    maxNodeOffset = nodeTable->getMaxNodeOffset(transaction);
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDVectorPos);
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDVectorPos);
    distanceVector = resultSet->getValueVector(distanceVectorPos);
    sspMorsel = std::make_unique<SSPMorsel>(maxNodeOffset, upperBound);
    initLocalRecursivePlan(context);
    bfsScanState.resetState();
}

bool RecursiveJoin::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (bfsScanState.sizeScanned < sspMorsel->numVisitedNodes) {
            // Scan from ssp morsel visited nodes.
            auto sizeToScan = std::min<uint64_t>(common::DEFAULT_VECTOR_CAPACITY,
                sspMorsel->numVisitedNodes - bfsScanState.sizeScanned);
            scanDstNodes(sizeToScan);
            bfsScanState.sizeScanned += sizeToScan;
            return true;
        }
        // Compute visited nodes through BFS.
        if (!computeBFS(context)) {
            return false;
        }
    }
}

bool RecursiveJoin::computeBFS(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    sspMorsel->resetState();
    assert(srcNodeIDVector->state->isFlat());
    auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
        srcNodeIDVector->state->selVector->selectedPositions[0]);
    sspMorsel->markSrc(nodeID.offset);
    while (!sspMorsel->isComplete()) {
        auto nodeOffset = sspMorsel->getNextNodeOffset();
        if (nodeOffset != common::INVALID_NODE_OFFSET) {
            // Found a starting node from current BFS level.
            scanBFSLevel->setNodeID(common::nodeID_t{nodeOffset, nodeTable->getTableID()});
            while (root->getNextTuple(context)) { // Exhaust recursive plan.
                updateVisitedState();
            }
        } else {
            // Otherwise move to the next BFS level.
            sspMorsel->moveNextLevelAsCurrentLevel();
        }
    }
    sspMorsel->unMarkSrc();
    bfsScanState.resetState();
    return true;
}

void RecursiveJoin::updateVisitedState() {
    auto visitedNodes = sspMorsel->visitedNodes;
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        if (visitedNodes[nodeID.offset] == VisitedState::NOT_VISITED) {
            sspMorsel->markVisited(nodeID.offset);
        }
    }
}

void RecursiveJoin::initLocalRecursivePlan(ExecutionContext* context) {
    auto op = root.get();
    while (!op->isSource()) {
        assert(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    scanBFSLevel = (ScanBFSLevel*)op;

    // Create two datachunks each with 1 nodeID value vector.
    // DataChunk 1 has flat state and contains the temporary src node ID vector for recursive join.
    // DataChunk 2 has unFlat state and contains the temporary dst node ID vector for recursive
    // join.
    localResultSet = std::make_unique<ResultSet>(2);
    localResultSet->insert(0, std::make_shared<common::DataChunk>(1));
    localResultSet->insert(1, std::make_shared<common::DataChunk>(1));
    localResultSet->dataChunks[0]->state = common::DataChunkState::getSingleValueDataChunkState();
    localResultSet->dataChunks[0]->insert(
        0, std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr));
    tmpDstNodeIDVector = std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr);
    localResultSet->dataChunks[1]->insert(0, tmpDstNodeIDVector);
    root->initLocalState(localResultSet.get(), context);
}

void RecursiveJoin::scanDstNodes(size_t sizeToScan) {
    auto size = 0;
    while (sizeToScan != size) {
        if (sspMorsel->visitedNodes[bfsScanState.currentOffset] == NOT_VISITED) {
            bfsScanState.currentOffset++;
            continue;
        }
        dstNodeIDVector->setValue<common::nodeID_t>(
            size, common::nodeID_t{bfsScanState.currentOffset, nodeTable->getTableID()});
        distanceVector->setValue<int64_t>(size, sspMorsel->distance[bfsScanState.currentOffset]);
        size++;
        bfsScanState.currentOffset++;
    }
    dstNodeIDVector->state->initOriginalAndSelectedSize(sizeToScan);
}

} // namespace processor
} // namespace kuzu
