#include "processor/operator/recursive_extend/recursive_join.h"

#include "processor/operator/recursive_extend/all_shortest_path_state.h"
#include "processor/operator/recursive_extend/shortest_path_state.h"
#include "processor/operator/recursive_extend/variable_length_state.h"
#include "processor/operator/scan/offset_scan_node_table.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::vector<NodeSemiMask*> RecursiveJoin::getSemiMask() const {
    std::vector<NodeSemiMask*> result;
    for (auto& mask : sharedState->semiMasks) {
        result.push_back(mask.get());
    }
    return result;
}

void RecursiveJoin::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    auto& dataInfo = info.dataInfo;
    populateTargetDstNodes(context);
    vectors = std::make_unique<RecursiveJoinVectors>();
    vectors->srcNodeIDVector = resultSet->getValueVector(dataInfo.srcNodePos).get();
    vectors->dstNodeIDVector = resultSet->getValueVector(dataInfo.dstNodePos).get();
    vectors->pathLengthVector = resultSet->getValueVector(dataInfo.pathLengthPos).get();
    auto semantic = context->clientContext->getClientConfig()->recursivePatternSemantic;
    path_semantic_check_func_t semanticCheck = nullptr;
    std::vector<std::unique_ptr<BaseFrontierScanner>> scanners;
    auto joinType = info.joinType;
    auto lowerBound = info.lowerBound;
    auto upperBound = info.upperBound;
    switch (info.queryRelType) {
    case QueryRelType::VARIABLE_LENGTH_WALK: {
        switch (info.joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            switch (semantic) {
            case PathSemantic::TRAIL: {
                semanticCheck = PathScanner::trailSemanticCheck;
            } break;
            case PathSemantic::ACYCLIC: {
                semanticCheck = PathScanner::acyclicSemanticCheck;
            } break;
            default:
                semanticCheck = nullptr;
            }
            vectors->pathVector = resultSet->getValueVector(dataInfo.pathPos).get();
            bfsState = std::make_unique<VariableLengthState<true /* TRACK_PATH */>>(upperBound,
                targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i,
                    dataInfo.tableIDToName, semanticCheck, info.direction, info.extendFromSource));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            if (semantic != PathSemantic::WALK) {
                throw RuntimeException("Different path semantics for COUNT(*) optimization is not "
                                       "implemented. Try WALK semantic.");
            }
            bfsState = std::make_unique<VariableLengthState<false /* TRACK_PATH */>>(upperBound,
                targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(
                    std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    case QueryRelType::SHORTEST: {
        if (semantic != PathSemantic::WALK) {
            throw RuntimeException("Different path semantics for shortest path is not implemented. "
                                   "Try WALK semantic.");
        }
        switch (joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            vectors->pathVector = resultSet->getValueVector(dataInfo.pathPos).get();
            bfsState = std::make_unique<ShortestPathState<true /* TRACK_PATH */>>(upperBound,
                targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i,
                    dataInfo.tableIDToName, nullptr, info.direction, info.extendFromSource));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsState = std::make_unique<ShortestPathState<false /* TRACK_PATH */>>(upperBound,
                targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(
                    std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    case QueryRelType::ALL_SHORTEST: {
        if (semantic != PathSemantic::WALK) {
            throw RuntimeException("Different path semantics for all shortest path is not "
                                   "implemented. Try WALK semantic.");
        }
        switch (joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            vectors->pathVector = resultSet->getValueVector(dataInfo.pathPos).get();
            bfsState = std::make_unique<AllShortestPathState<true /* TRACK_PATH */>>(upperBound,
                targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i,
                    dataInfo.tableIDToName, nullptr, info.direction, info.extendFromSource));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsState = std::make_unique<AllShortestPathState<false /* TRACK_PATH */>>(upperBound,
                targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(
                    std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (vectors->pathVector != nullptr) {
        auto pathNodesFieldIdx =
            StructType::getFieldIdx(vectors->pathVector->dataType, InternalKeyword::NODES);
        vectors->pathNodesVector =
            StructVector::getFieldVector(vectors->pathVector, pathNodesFieldIdx).get();
        auto pathNodesDataVector = ListVector::getDataVector(vectors->pathNodesVector);
        auto pathNodesIDFieldIdx =
            StructType::getFieldIdx(pathNodesDataVector->dataType, InternalKeyword::ID);
        vectors->pathNodesIDDataVector =
            StructVector::getFieldVector(pathNodesDataVector, pathNodesIDFieldIdx).get();
        auto pathNodesLabelFieldIdx =
            StructType::getFieldIdx(pathNodesDataVector->dataType, InternalKeyword::LABEL);
        vectors->pathNodesLabelDataVector =
            StructVector::getFieldVector(pathNodesDataVector, pathNodesLabelFieldIdx).get();
        auto pathRelsFieldIdx =
            StructType::getFieldIdx(vectors->pathVector->dataType, InternalKeyword::RELS);
        vectors->pathRelsVector =
            StructVector::getFieldVector(vectors->pathVector, pathRelsFieldIdx).get();
        auto pathRelsDataVector = ListVector::getDataVector(vectors->pathRelsVector);
        auto pathRelsSrcIDFieldIdx =
            StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::SRC);
        vectors->pathRelsSrcIDDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsSrcIDFieldIdx).get();
        auto pathRelsDstIDFieldIdx =
            StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::DST);
        vectors->pathRelsDstIDDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsDstIDFieldIdx).get();
        auto pathRelsIDFieldIdx =
            StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::ID);
        vectors->pathRelsIDDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsIDFieldIdx).get();
        auto pathRelsLabelFieldIdx =
            StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::LABEL);
        vectors->pathRelsLabelDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsLabelFieldIdx).get();
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
    sel_t offsetVectorSize = 0u;
    sel_t nodeIDDataVectorSize = 0u;
    sel_t relIDDataVectorSize = 0u;
    if (vectors->pathVector != nullptr) {
        vectors->pathVector->resetAuxiliaryBuffer();
    }
    frontiersScanner->scan(*vectors, offsetVectorSize, nodeIDDataVectorSize, relIDDataVectorSize);
    if (offsetVectorSize == 0) {
        return false;
    }
    vectors->dstNodeIDVector->state->initOriginalAndSelectedSize(offsetVectorSize);
    return true;
}

void RecursiveJoin::computeBFS(ExecutionContext* context) {
    auto nodeID = vectors->srcNodeIDVector->getValue<nodeID_t>(
        vectors->srcNodeIDVector->state->getSelVector()[0]);
    bfsState->markSrc(nodeID);
    vectors->recursiveNodePredicateExecFlagVector->setValue<bool>(0, true);
    while (!bfsState->isComplete()) {
        auto boundNodeID = bfsState->getNextNodeID();
        if (boundNodeID.offset != INVALID_OFFSET) {
            // Found a starting node from current frontier.
            recursiveSource->init(boundNodeID);
            while (recursiveRoot->getNextTuple(context)) { // Exhaust recursive plan.
                updateVisitedNodes(boundNodeID);
            }
        } else {
            // Otherwise move to the next frontier.
            bfsState->finalizeCurrentLevel();
            vectors->recursiveNodePredicateExecFlagVector->setValue<bool>(0, false);
        }
    }
}

void RecursiveJoin::updateVisitedNodes(nodeID_t boundNodeID) {
    auto boundNodeMultiplicity = bfsState->getMultiplicity(boundNodeID);
    auto& selVector = vectors->recursiveDstNodeIDVector->state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); ++i) {
        auto pos = selVector[i];
        auto nbrNodeID = vectors->recursiveDstNodeIDVector->getValue<nodeID_t>(pos);
        auto edgeID = vectors->recursiveEdgeIDVector->getValue<relID_t>(pos);
        if (vectors->recursiveEdgeDirectionVector != nullptr) {
            if (vectors->recursiveEdgeDirectionVector->getValue<bool>(pos)) {
                RelIDMasker::markFlip(edgeID);
            }
        }
        bfsState->markVisited(boundNodeID, nbrNodeID, edgeID, boundNodeMultiplicity);
    }
}

static PhysicalOperator* getSource(PhysicalOperator* op) {
    while (op->getNumChildren() != 0) {
        KU_ASSERT(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    return op;
}

void RecursiveJoin::initLocalRecursivePlan(ExecutionContext* context) {
    auto& dataInfo = info.dataInfo;
    localResultSet = std::make_unique<ResultSet>(dataInfo.localResultSetDescriptor.get(),
        context->clientContext->getMemoryManager());
    vectors->recursiveDstNodeIDVector =
        localResultSet->getValueVector(dataInfo.recursiveDstNodeIDPos).get();
    vectors->recursiveNodePredicateExecFlagVector =
        localResultSet->getValueVector(dataInfo.recursiveNodePredicateExecFlagPos).get();
    vectors->recursiveEdgeIDVector =
        localResultSet->getValueVector(dataInfo.recursiveEdgeIDPos).get();
    if (dataInfo.recursiveEdgeDirectionPos.isValid()) {
        vectors->recursiveEdgeDirectionVector =
            localResultSet->getValueVector(dataInfo.recursiveEdgeDirectionPos).get();
    }
    recursiveRoot->initLocalState(localResultSet.get(), context);
    recursiveSource = getSource(recursiveRoot.get())->ptrCast<OffsetScanNodeTable>();
}

void RecursiveJoin::populateTargetDstNodes(ExecutionContext*) {
    node_id_set_t targetNodeIDs;
    uint64_t numTargetNodes = 0;
    for (auto& mask : sharedState->semiMasks) {
        auto numNodes = mask->getMaxOffset() + 1;
        if (mask->isEnabled()) {
            for (auto offset = 0u; offset < numNodes; ++offset) {
                if (mask->isMasked(offset)) {
                    targetNodeIDs.insert(nodeID_t{offset, mask->getTableID()});
                    numTargetNodes++;
                }
            }
        } else {
            KU_ASSERT(targetNodeIDs.empty());
            numTargetNodes += numNodes;
        }
    }
    targetDstNodes = std::make_unique<TargetDstNodes>(numTargetNodes, std::move(targetNodeIDs));
    auto& dataInfo = info.dataInfo;
    for (auto tableID : dataInfo.recursiveDstNodeTableIDs) {
        if (!dataInfo.dstNodeTableIDs.contains(tableID)) {
            targetDstNodes->setTableIDFilter(dataInfo.dstNodeTableIDs);
            return;
        }
    }
}

} // namespace processor
} // namespace kuzu
