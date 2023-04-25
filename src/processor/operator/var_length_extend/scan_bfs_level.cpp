#include "processor/operator/var_length_extend/scan_bfs_level.h"

namespace kuzu {
namespace processor {

void ScanBFSLevel::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    maxNodeOffset = nodeTable->getMaxNodeOffset(transaction);
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDVectorPos);
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDVectorPos);
    distanceVector = resultSet->getValueVector(distanceVectorPos);

    threadLocalSharedState = std::make_shared<SSPThreadLocalSharedState>();
    threadLocalSharedState->sspMorsel = std::make_unique<SSPMorsel>(maxNodeOffset);
    threadLocalSharedState->sspMorsel->resetState(maxNodeOffset);
    sizeScanned = 0;
    currentOffset = 0;
    auto op = root.get();
    while (!op->isSource()) {
        assert(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    dummyScan = (DummyScan*)op;

    localResultSet = std::make_unique<ResultSet>(2);
    localResultSet->insert(0, std::make_shared<common::DataChunk>(1));
    localResultSet->insert(1, std::make_shared<common::DataChunk>(1));
    localResultSet->dataChunks[0]->state = common::DataChunkState::getSingleValueDataChunkState();
    localResultSet->dataChunks[0]->insert(0, std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr));
    localResultSet->dataChunks[1]->insert(0, std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr));
    root->initLocalState(localResultSet.get(), context);
}

bool ScanBFSLevel::getNextTuplesInternal(ExecutionContext* context) {
    auto sspMorsel = threadLocalSharedState->sspMorsel.get();
    while (true) {
        if (sizeScanned < sspMorsel->numVisitedNodes) { // scan from
            auto sizeToScan = std::min<uint64_t>(
                common::DEFAULT_VECTOR_CAPACITY, sspMorsel->numVisitedNodes - sizeScanned);
            auto size = 0;
            while (sizeToScan != size) {
                if (sspMorsel->visitedNodes[currentOffset] == NOT_VISITED) {
                    currentOffset++;
                    continue;
                }
                dstNodeIDVector->setValue<common::nodeID_t>(
                    size, common::nodeID_t{currentOffset, nodeTable->getTableID()});
                distanceVector->setValue<int64_t>(size, sspMorsel->distance[currentOffset]);
                size++;
                currentOffset++;
            }
            dstNodeIDVector->state->initOriginalAndSelectedSize(sizeToScan);
            sizeScanned += sizeToScan;
            return true;
        }
        // compute
        if (!computeBFS(context)) {
            return false;
        }
    }
}

bool ScanBFSLevel::computeBFS(ExecutionContext* context) {
    auto sspMorsel = threadLocalSharedState->sspMorsel.get();
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    sspMorsel->resetState(maxNodeOffset);
    assert(srcNodeIDVector->state->isFlat());
    auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
        srcNodeIDVector->state->selVector->selectedPositions[0]);
    sspMorsel->markSrc(nodeID.offset);
    while (!sspMorsel->isComplete(upperBound, maxNodeOffset)) {
        auto nodeOffset = sspMorsel->getNextNodeOffset();
        if (nodeOffset != common::INVALID_NODE_OFFSET) {
            // Found a starting node from current BFS level.
            dummyScan->setNodeID(common::nodeID_t{nodeOffset, nodeTable->getTableID()});
            while (root->getNextTuple(context)) {
                updateVisitedState();
            }
        } else {
            // Otherwise move to the next BFS level.
            sspMorsel->moveNextLevelAsCurrentLevel(upperBound);
        }
    }
    sspMorsel->visitedNodes[sspMorsel->startOffset] = NOT_VISITED;
    sspMorsel->numVisitedNodes--;
    sizeScanned = 0;
    currentOffset = 0;
    return true;
}

} // namespace processor
} // namespace kuzu
