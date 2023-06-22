#include "processor/operator/recursive_extend/recursive_join.h"

#include "processor/operator/recursive_extend/all_shortest_path_state.h"
#include "processor/operator/recursive_extend/scan_frontier.h"
#include "processor/operator/recursive_extend/shortest_path_state.h"
#include "processor/operator/recursive_extend/variable_length_state.h"

namespace kuzu {
namespace processor {

void RecursiveJoin::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    threadIdx = morselDispatcher->getThreadIdx();
    for (auto& dataPos : vectorsToScanPos) {
        vectorsToScan.push_back(resultSet->getValueVector(dataPos).get());
    }
    populateTargetDstNodes();
    vectors = std::make_unique<RecursiveJoinVectors>();
    vectors->srcNodeIDVector = resultSet->getValueVector(dataInfo->srcNodePos).get();
    vectors->dstNodeIDVector = resultSet->getValueVector(dataInfo->dstNodePos).get();
    vectors->pathLengthVector = resultSet->getValueVector(dataInfo->pathLengthPos).get();
    std::vector<std::unique_ptr<BaseFrontierScanner>> scanners;
    switch (queryRelType) {
    case common::QueryRelType::VARIABLE_LENGTH: {
        switch (joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            vectors->pathVector = resultSet->getValueVector(dataInfo->pathPos).get();
            bfsMorsel = std::make_unique<VariableLengthMorsel<true /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsMorsel = std::make_unique<VariableLengthMorsel<false /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(
                    std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            throw common::NotImplementedException("BaseRecursiveJoin::initLocalStateInternal");
        }
    } break;
    case common::QueryRelType::SHORTEST: {
        switch (joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            vectors->pathVector = resultSet->getValueVector(dataInfo->pathPos).get();
            bfsMorsel = std::make_unique<ShortestPathMorsel<true /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsMorsel = std::make_unique<ShortestPathMorsel<false /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<DstNodeScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            throw common::NotImplementedException("BaseRecursiveJoin::initLocalStateInternal");
        }
    } break;
    case common::QueryRelType::ALL_SHORTEST: {
        switch (joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            vectors->pathVector = resultSet->getValueVector(dataInfo->pathPos).get();
            bfsMorsel = std::make_unique<AllShortestPathMorsel<true /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsMorsel = std::make_unique<AllShortestPathMorsel<false /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(
                    std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            throw common::NotImplementedException("BaseRecursiveJoin::initLocalStateInternal");
        }
    } break;
    default:
        throw common::NotImplementedException("BaseRecursiveJoin::initLocalStateInternal");
    }
    if (vectors->pathVector != nullptr) {
        assert(vectors->pathVector->dataType.getPhysicalType() == common::PhysicalTypeID::STRUCT);
        auto nodeIDFieldIdx = common::StructType::getFieldIdx(
            &vectors->pathVector->dataType, common::InternalKeyword::NODES);
        auto relIDFieldIdx = common::StructType::getFieldIdx(
            &vectors->pathVector->dataType, common::InternalKeyword::RELS);
        vectors->pathNodeIDVector =
            common::StructVector::getFieldVector(vectors->pathVector, nodeIDFieldIdx).get();
        vectors->pathRelIDVector =
            common::StructVector::getFieldVector(vectors->pathVector, relIDFieldIdx).get();
    }
    frontiersScanner = std::make_unique<FrontiersScanner>(std::move(scanners));
    initLocalRecursivePlan(context);
}

bool RecursiveJoin::getNextTuplesInternal(ExecutionContext* context) {
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
        if (!computeBFSTemp(context)) {
            return false;
        }
        frontiersScanner->resetState(*bfsMorsel);
    }
}

bool RecursiveJoin::scanOutput() {
    if (morselDispatcher->getSchedulerType() == SchedulerType::OneThreadOneMorsel) {
        common::sel_t offsetVectorSize = 0u;
        common::sel_t nodeIDDataVectorSize = 0u;
        common::sel_t relIDDataVectorSize = 0u;
        if (vectors->pathVector != nullptr) {
            vectors->pathVector->resetAuxiliaryBuffer();
        }
        frontiersScanner->scan(
            vectors.get(), offsetVectorSize, nodeIDDataVectorSize, relIDDataVectorSize);
        if (offsetVectorSize == 0) {
            return false;
        }
        vectors->dstNodeIDVector->state->initOriginalAndSelectedSize(offsetVectorSize);
        return true;
    } else {
        while (true) {
            auto tableID = *begin(dataInfo->dstNodeTableIDs);
            auto ret = morselDispatcher->writeDstNodeIDAndDistance(
                sharedState->inputFTableSharedState, vectorsToScan, colIndicesToScan,
                vectors->dstNodeIDVector, vectors->pathLengthVector, tableID, threadIdx);
            /**
             * ret > 0: non-zero path lengths were written to vector, return to parent op
             * ret < 0: path length writing is complete, proceed to computeBFS for another morsel
             * ret = 0: the distance morsel received was empty, go back to get another morsel
             */
            if (ret > 0) {
                return true;
            } else if (ret == 0) {
                continue;
            } else {
                return false;
            }
        }
    }
}

bool RecursiveJoin::computeBFSTemp(kuzu::processor::ExecutionContext* context) {
    while (true) {
        if (bfsMorsel->ssspMorsel) {
            bfsMorsel->ssspMorsel->getBFSMorsel(bfsMorsel);
            if (!bfsMorsel->threadCheckSSSPState) {
                extend(context);
                if (bfsMorsel->ssspMorsel->finishBFSMorsel(bfsMorsel)) {
                    return true;
                }
                continue;
            }
        }
        auto status = fetchBFSMorselFromDispatcher(context);
        if (status == -1) {
            return false;
        } else if (status == 0) {
            continue;
        } else {
            return true;
        }
    }
}

/*
 * returns -1 = exit, return false from operator, all work done
 * returns 0 = keep asking for work
 * returns 1 = SSSP extend is complete, can return to writing distances
 */
int RecursiveJoin::fetchBFSMorselFromDispatcher(ExecutionContext* context) {
    auto bfsComputationState = morselDispatcher->getBFSMorsel(sharedState->inputFTableSharedState,
        vectorsToScan, colIndicesToScan, vectors->srcNodeIDVector, bfsMorsel, threadIdx);
    auto globalSSSPState = bfsComputationState.first;
    auto ssspLocalState = bfsComputationState.second;
    if (bfsMorsel->threadCheckSSSPState) {
        switch (globalSSSPState) {
        case IN_PROGRESS:
        case IN_PROGRESS_ALL_SRC_SCANNED:
            if (ssspLocalState == MORSEL_COMPLETE || ssspLocalState == MORSEL_EXTEND_IN_PROGRESS) {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(common::THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
                return 0;
            } else {
                return 1;
            }
        case COMPLETE:
            return -1;
        default:
            assert(false);
        }
    }
    if (morselDispatcher->getSchedulerType() == OneThreadOneMorsel) {
        computeBFS(context);
        return 1;
    } else {
        extend(context);
        if (bfsMorsel->ssspMorsel->finishBFSMorsel(bfsMorsel)) {
            return 1;
        } else {
            return 0;
        }
    }
}

void RecursiveJoin::computeBFS(ExecutionContext* context) {
    while (!bfsMorsel->isComplete()) {
        auto boundNodeID = bfsMorsel->getNextNodeID();
        if (boundNodeID.offset != common::INVALID_OFFSET) {
            // Found a starting node from current frontier.
            scanFrontier->setNodeID(boundNodeID);
            while (recursiveRoot->getNextTuple(context)) { // Exhaust recursive plan.
                updateVisitedNodes(boundNodeID);
            }
        } else {
            // Otherwise move to the next frontier.
            bfsMorsel->finalizeCurrentLevel();
        }
    }
}

void RecursiveJoin::updateVisitedNodes(common::nodeID_t boundNodeID) {
    auto boundNodeMultiplicity = bfsMorsel->getMultiplicity(boundNodeID);
    for (auto i = 0u; i < vectors->recursiveDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = vectors->recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nbrNodeID = vectors->recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto edgeID = vectors->recursiveEdgeIDVector->getValue<common::relID_t>(pos);
        bfsMorsel->markVisited(boundNodeID, nbrNodeID, edgeID, boundNodeMultiplicity);
    }
}

void RecursiveJoin::extend(ExecutionContext* context) {
    auto shortestPathMorsel = reinterpret_cast<ShortestPathMorsel<false>*>(bfsMorsel.get());
    common::offset_t nodeOffset = shortestPathMorsel->getNextNodeOffset();
    while (nodeOffset != common::INVALID_OFFSET) {
        scanFrontier->setNodeID(common::nodeID_t{nodeOffset, *begin(dataInfo->dstNodeTableIDs)});
        while (recursiveRoot->getNextTuple(context)) { // Exhaust recursive plan.
            shortestPathMorsel->addToLocalNextBFSLevel(vectors->recursiveDstNodeIDVector);
        }
        nodeOffset = shortestPathMorsel->getNextNodeOffset();
    }
}

void RecursiveJoin::initLocalRecursivePlan(ExecutionContext* context) {
    auto op = recursiveRoot.get();
    while (!op->isSource()) {
        assert(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    scanFrontier = (ScanFrontier*)op;
    localResultSet = std::make_unique<ResultSet>(
        dataInfo->localResultSetDescriptor.get(), context->memoryManager);
    vectors->recursiveDstNodeIDVector =
        localResultSet->getValueVector(dataInfo->recursiveDstNodeIDPos).get();
    vectors->recursiveEdgeIDVector =
        localResultSet->getValueVector(dataInfo->recursiveEdgeIDPos).get();
    recursiveRoot->initLocalState(localResultSet.get(), context);
}

void RecursiveJoin::populateTargetDstNodes() {
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
