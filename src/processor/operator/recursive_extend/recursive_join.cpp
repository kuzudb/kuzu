#include "processor/operator/recursive_extend/recursive_join.h"

#include "processor/operator/recursive_extend/all_shortest_path_state.h"
#include "processor/operator/recursive_extend/scan_frontier.h"
#include "processor/operator/recursive_extend/shortest_path_state.h"
#include "processor/operator/recursive_extend/variable_length_state.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void RecursiveJoin::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
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
            bfsState = std::make_unique<VariableLengthState<true /* TRACK_PATH */>>(
                upperBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsState = std::make_unique<VariableLengthState<false /* TRACK_PATH */>>(
                upperBound, targetDstNodes.get());
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
            bfsState = std::make_unique<ShortestPathState<true /* TRACK_PATH */>>(
                upperBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsState = std::make_unique<ShortestPathState<false /* TRACK_PATH */>>(
                upperBound, targetDstNodes.get());
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
            bfsState = std::make_unique<AllShortestPathState<true /* TRACK_PATH */>>(
                upperBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsState = std::make_unique<AllShortestPathState<false /* TRACK_PATH */>>(
                upperBound, targetDstNodes.get());
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
        auto pathNodesFieldIdx = common::StructType::getFieldIdx(
            &vectors->pathVector->dataType, common::InternalKeyword::NODES);
        vectors->pathNodesVector =
            StructVector::getFieldVector(vectors->pathVector, pathNodesFieldIdx).get();
        auto pathNodesDataVector = ListVector::getDataVector(vectors->pathNodesVector);
        auto pathNodesIDFieldIdx =
            StructType::getFieldIdx(&pathNodesDataVector->dataType, InternalKeyword::ID);
        vectors->pathNodesIDDataVector =
            StructVector::getFieldVector(pathNodesDataVector, pathNodesIDFieldIdx).get();
        assert(vectors->pathNodesIDDataVector->dataType.getPhysicalType() ==
               common::PhysicalTypeID::INTERNAL_ID);
        auto pathRelsFieldIdx = common::StructType::getFieldIdx(
            &vectors->pathVector->dataType, common::InternalKeyword::RELS);
        vectors->pathRelsVector =
            StructVector::getFieldVector(vectors->pathVector, pathRelsFieldIdx).get();
        auto pathRelsDataVector = ListVector::getDataVector(vectors->pathRelsVector);
        auto pathRelsIDFieldIdx =
            StructType::getFieldIdx(&pathRelsDataVector->dataType, InternalKeyword::ID);
        vectors->pathRelsIDDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsIDFieldIdx).get();
        assert(vectors->pathRelsIDDataVector->dataType.getPhysicalType() ==
               common::PhysicalTypeID::INTERNAL_ID);
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
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        bfsState->resetState();
        computeBFS(context); // Phase 1
        frontiersScanner->resetState(*bfsState);
    }
}

bool RecursiveJoin::scanOutput() {
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
}

void RecursiveJoin::computeBFS(ExecutionContext* context) {
    auto nodeID = vectors->srcNodeIDVector->getValue<common::nodeID_t>(
        vectors->srcNodeIDVector->state->selVector->selectedPositions[0]);
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

void RecursiveJoin::updateVisitedNodes(common::nodeID_t boundNodeID) {
    auto boundNodeMultiplicity = bfsState->getMultiplicity(boundNodeID);
    for (auto i = 0u; i < vectors->recursiveDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = vectors->recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nbrNodeID = vectors->recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto edgeID = vectors->recursiveEdgeIDVector->getValue<common::relID_t>(pos);
        bfsState->markVisited(boundNodeID, nbrNodeID, edgeID, boundNodeMultiplicity);
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
