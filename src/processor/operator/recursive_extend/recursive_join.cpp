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
    pathLengthVector = resultSet->getValueVector(dataInfo->pathLengthPos).get();
    switch (dataInfo->joinType) {
    case planner::RecursiveJoinType::TRACK_PATH: {
        pathVector = resultSet->getValueVector(dataInfo->pathPos).get();
    } break;
    default: {
        pathVector = nullptr;
    } break;
    }
    initLocalRecursivePlan(context);
    populateTargetDstNodes();
}

bool BaseRecursiveJoin::getNextTuplesInternal(ExecutionContext* context) {
    if (targetDstNodes->getNumNodes() == 0) {
        return false;
    }
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
        bfsState->resetState();
        computeBFS(context); // Phase 1
        frontiersScanner->resetState(*bfsState);
    }
}

bool BaseRecursiveJoin::scanOutput() {
    common::sel_t offsetVectorSize = 0u;
    common::sel_t dataVectorSize = 0u;
    if (pathVector != nullptr) {
        pathVector->resetAuxiliaryBuffer();
    }
    frontiersScanner->scan(
        pathVector, dstNodeIDVector, pathLengthVector, offsetVectorSize, dataVectorSize);
    if (offsetVectorSize == 0) {
        return false;
    }
    dstNodeIDVector->state->initOriginalAndSelectedSize(offsetVectorSize);
    return true;
}

void BaseRecursiveJoin::computeBFS(ExecutionContext* context) {
    auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
        srcNodeIDVector->state->selVector->selectedPositions[0]);
    bfsState->markSrc(nodeID);
    while (!bfsState->isComplete()) {
        auto boundNodeID = bfsState->getNextNodeID();
        if (boundNodeID.offset != common::INVALID_OFFSET) {
            // Found a starting node from current frontier.
            scanFrontier->setNodeID(boundNodeID);
            while (recursiveRoot->getNextTuple(context)) { // Exhaust recursive plan.
                updateVisitedNodes(boundNodeID);
            }
        } else {
            // Otherwise move to the next frontier.
            bfsState->finalizeCurrentLevel();
        }
    }
}

void BaseRecursiveJoin::updateVisitedNodes(common::nodeID_t boundNodeID) {
    auto boundNodeMultiplicity = bfsState->getMultiplicity(boundNodeID);
    for (auto i = 0u; i < recursiveDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nbrNodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto edgeID = recursiveEdgeIDVector->getValue<common::relID_t>(pos);
        bfsState->markVisited(boundNodeID, nbrNodeID, edgeID, boundNodeMultiplicity);
    }
}

void BaseRecursiveJoin::initLocalRecursivePlan(ExecutionContext* context) {
    auto op = recursiveRoot.get();
    while (!op->isSource()) {
        assert(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    scanFrontier = (ScanFrontier*)op;
    localResultSet = std::make_unique<ResultSet>(
        dataInfo->localResultSetDescriptor.get(), context->memoryManager);
    recursiveDstNodeIDVector =
        localResultSet->getValueVector(dataInfo->recursiveDstNodeIDPos).get();
    recursiveEdgeIDVector = localResultSet->getValueVector(dataInfo->recursiveEdgeIDPos).get();
    recursiveRoot->initLocalState(localResultSet.get(), context);
}

void BaseRecursiveJoin::populateTargetDstNodes() {
    frontier::node_id_set_t targetNodeIDs;
    uint64_t numTargetNodes = 0;
    for (auto& semiMask : sharedState->semiMasks) {
        auto nodeTable = semiMask->getNodeTable();
        auto numNodes = nodeTable->getMaxNodeOffset(transaction) + 1;
        if (semiMask->isEnabled()) {
            for (auto offset = 0u; offset < numNodes; ++offset) {
                if (semiMask->isNodeMasked(offset)) {
                    targetNodeIDs.insert(common::nodeID_t{offset, nodeTable->getTableID()});
                    numTargetNodes++;
                }
            }
        } else {
            assert(targetNodeIDs.empty());
            numTargetNodes += numNodes;
        }
    }
    targetDstNodes = std::make_unique<TargetDstNodes>(numTargetNodes, std::move(targetNodeIDs));
    for (auto tableID : dataInfo->recursiveDstNodeTableIDs) {
        if (!dataInfo->dstNodeTableIDs.contains(tableID)) {
            targetDstNodes->setTableIDFilter(dataInfo->dstNodeTableIDs);
            return;
        }
    }
}

} // namespace processor
} // namespace kuzu
