#include "processor/operator/recursive_extend/frontier_scanner.h"

#include "processor/operator/recursive_extend/recursive_join.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

size_t BaseFrontierScanner::scan(RecursiveJoinVectors& vectors, sel_t& vectorPos,
    sel_t& nodeIDDataVectorPos, sel_t& relIDDataVectorPos) {
    if (k >= frontiers.size()) {
        // BFS terminate before current depth. No need to scan.
        return 0;
    }
    auto vectorPosBeforeScanning = vectorPos;
    auto lastFrontier = frontiers[k];
    while (true) {
        if (currentDstNodeID.offset != INVALID_OFFSET) {
            scanFromDstOffset(vectors, vectorPos, nodeIDDataVectorPos, relIDDataVectorPos);
        }
        if (vectorPos == DEFAULT_VECTOR_CAPACITY) {
            break;
        }
        if (lastFrontierCursor == lastFrontier->nodeIDs.size()) {
            // All nodes from last frontier have been scanned.
            currentDstNodeID = {INVALID_OFFSET, INVALID_TABLE_ID};
            break;
        }
        currentDstNodeID = lastFrontier->nodeIDs[lastFrontierCursor++];
        // Skip nodes that is not in semi mask.
        if (!targetDstNodes->contains(currentDstNodeID)) {
            currentDstNodeID.offset = INVALID_OFFSET;
            continue;
        }
        initScanFromDstOffset();
    }
    return vectorPos - vectorPosBeforeScanning;
}

void BaseFrontierScanner::resetState(const BaseBFSState& bfsState) {
    lastFrontierCursor = 0;
    currentDstNodeID = {INVALID_OFFSET, INVALID_TABLE_ID};
    frontiers.clear();
    for (auto i = 0u; i < bfsState.getNumFrontiers(); ++i) {
        frontiers.push_back(bfsState.getFrontier(i));
    }
}

bool PathScanner::trailSemanticCheck(const std::vector<nodeID_t>&,
    const std::vector<relID_t>& edgeIDs) {
    common::rel_id_set_t set;
    for (auto i = 0u; i < edgeIDs.size() - 1; ++i) {
        internalID_t edgeID = edgeIDs[i];
        if (RelIDMasker::needFlip(edgeID)) {
            edgeID = RelIDMasker::getWithoutMark(edgeID);
        }
        if (set.contains(edgeID)) {
            return false;
        }
        set.insert(edgeID);
    }
    return true;
}

bool PathScanner::acyclicSemanticCheck(const std::vector<nodeID_t>& nodeIDs,
    const std::vector<relID_t>&) {
    common::node_id_set_t set;
    for (auto i = 0u; i < nodeIDs.size(); ++i) {
        if (set.contains(nodeIDs[i])) {
            return false;
        }
        set.insert(nodeIDs[i]);
    }
    return true;
}

void PathScanner::scanFromDstOffset(RecursiveJoinVectors& vectors, sel_t& vectorPos,
    sel_t& nodeIDDataVectorPos, sel_t& relIDDataVectorPos) {
    // when bound node is 0
    if (k == 0) {
        writePathToVector(vectors, vectorPos, nodeIDDataVectorPos, relIDDataVectorPos);
        return;
    }

    auto level = 0;
    while (!nbrsStack.empty()) {
        auto& cursor = cursorStack.top();
        cursor++;
        if ((uint64_t)cursor < nbrsStack.top()->size()) { // Found a new nbr
            auto& nbr = nbrsStack.top()->at(cursor);
            nodeIDs[level] = nbr.first;
            relIDs[level] = nbr.second;
            if (level == 0) { // Found a new nbr at level 0. Found a new path.
                writePathToVector(vectors, vectorPos, nodeIDDataVectorPos, relIDDataVectorPos);
                if (vectorPos == DEFAULT_VECTOR_CAPACITY) {
                    return;
                }
                continue;
            }
            // Push new stack.
            cursorStack.push(-1);
            nbrsStack.push(&frontiers[level]->bwdEdges.at(nbr.first));
            level--;
        } else { // Failed to find a nbr. Pop stack.
            cursorStack.pop();
            nbrsStack.pop();
            level++;
        }
    }
}

void PathScanner::initDfs(const node_rel_id_t& nodeAndRelID, size_t currentDepth) {
    nodeIDs[currentDepth] = nodeAndRelID.first;
    relIDs[currentDepth] = nodeAndRelID.second;
    if (k == 0) {
        return;
    }
    if (currentDepth == 0) {
        cursorStack.top() = -1;
        return;
    }
    auto nbrs = &frontiers[currentDepth]->bwdEdges.at(nodeAndRelID.first);
    nbrsStack.push(nbrs);
    cursorStack.push(0);
    initDfs(nbrs->at(0), currentDepth - 1);
}

void PathScanner::writePathNode(idx_t idx, RecursiveJoinVectors& vectors, sel_t vectorPos) {
    auto nodeID = nodeIDs[idx];
    vectors.pathNodesIDDataVector->setValue<nodeID_t>(vectorPos, nodeID);
    auto labelName = tableIDToName.at(nodeID.tableID);
    StringVector::addString(vectors.pathNodesLabelDataVector, vectorPos, labelName.data(),
        labelName.length());
}

void PathScanner::writePathSrcDstNode(idx_t srcNodeIdx, idx_t dstNodeIdx,
    RecursiveJoinVectors& vectors, sel_t vectorPos) {
    vectors.pathRelsSrcIDDataVector->setValue<nodeID_t>(vectorPos, nodeIDs[srcNodeIdx]);
    vectors.pathRelsDstIDDataVector->setValue<nodeID_t>(vectorPos, nodeIDs[dstNodeIdx]);
}

void PathScanner::writePathRel(common::internalID_t relID, RecursiveJoinVectors& vectors,
    sel_t vectorPos) {
    vectors.pathRelsIDDataVector->setValue<relID_t>(vectorPos, relID);
    StringVector::addString(vectors.pathRelsLabelDataVector, vectorPos,
        tableIDToName.at(relID.tableID));
}

void PathScanner::writePathToVector(RecursiveJoinVectors& vectors, sel_t& vectorPos,
    sel_t& nodeIDDataVectorPos, sel_t& relIDDataVectorPos) {
    if (semanticCheckFunc && !semanticCheckFunc(nodeIDs, relIDs)) {
        return;
    }
    KU_ASSERT(vectorPos < DEFAULT_VECTOR_CAPACITY);
    // Allocate list entries.
    auto nodeTableEntry = ListVector::addList(vectors.pathNodesVector, k > 0 ? k - 1 : 0);
    auto relTableEntry = ListVector::addList(vectors.pathRelsVector, k);
    vectors.pathNodesVector->setValue(vectorPos, nodeTableEntry);
    vectors.pathRelsVector->setValue(vectorPos, relTableEntry);
    // Write dst
    writeDstNodeOffsetAndLength(vectors.dstNodeIDVector, vectors.pathLengthVector, vectorPos);
    vectorPos++;
    // Write path nodes.
    if (extendFromSource) {
        for (auto i = 1u; i < k; ++i) {
            writePathNode(i, vectors, nodeIDDataVectorPos);
            nodeIDDataVectorPos++;
        }
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            for (auto i = 0u; i < k; ++i) {
                writePathSrcDstNode(i, i + 1, vectors, relIDDataVectorPos);
                writePathRel(relIDs[i], vectors, relIDDataVectorPos);
                relIDDataVectorPos++;
            }
        } break;
        case ExtendDirection::BWD: {
            for (auto i = 0u; i < k; ++i) {
                writePathSrcDstNode(i + 1, i, vectors, relIDDataVectorPos);
                writePathRel(relIDs[i], vectors, relIDDataVectorPos);
                relIDDataVectorPos++;
            }
        } break;
        case ExtendDirection::BOTH: {
            for (auto i = 0u; i < k; ++i) {
                auto relID = relIDs[i];
                if (RelIDMasker::needFlip(relID)) {
                    writePathSrcDstNode(i + 1, i, vectors, relIDDataVectorPos);
                } else {
                    writePathSrcDstNode(i, i + 1, vectors, relIDDataVectorPos);
                }
                RelIDMasker::clearMark(relID);
                writePathRel(relID, vectors, relIDDataVectorPos);
                relIDDataVectorPos++;
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    } else {
        for (auto i = 1u; i < k; ++i) {
            writePathNode(k - i, vectors, nodeIDDataVectorPos);
            nodeIDDataVectorPos++;
        }
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            for (auto i = 0u; i < k; ++i) {
                writePathSrcDstNode(k - 1 - i, k - i, vectors, relIDDataVectorPos);
                writePathRel(relIDs[k - 1 - i], vectors, relIDDataVectorPos);
                relIDDataVectorPos++;
            }
        } break;
        case ExtendDirection::BWD: {
            for (auto i = 0u; i < k; ++i) {
                writePathSrcDstNode(k - i, k - 1 - i, vectors, relIDDataVectorPos);
                writePathRel(relIDs[k - 1 - i], vectors, relIDDataVectorPos);
                relIDDataVectorPos++;
            }
        } break;
        case ExtendDirection::BOTH: {
            for (auto i = 0u; i < k; ++i) {
                auto relID = relIDs[k - 1 - i];
                if (RelIDMasker::needFlip(relID)) {
                    writePathSrcDstNode(k - 1 - i, k - i, vectors, relIDDataVectorPos);
                } else {
                    writePathSrcDstNode(k - i, k - 1 - i, vectors, relIDDataVectorPos);
                }
                RelIDMasker::clearMark(relID);
                writePathRel(relID, vectors, relIDDataVectorPos);
                relIDDataVectorPos++;
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
}

void DstNodeWithMultiplicityScanner::scanFromDstOffset(RecursiveJoinVectors& vectors,
    sel_t& vectorPos, sel_t&, sel_t&) {
    auto& multiplicity = frontiers[k]->nodeIDToMultiplicity.at(currentDstNodeID);
    while (multiplicity > 0 && vectorPos < DEFAULT_VECTOR_CAPACITY) {
        writeDstNodeOffsetAndLength(vectors.dstNodeIDVector, vectors.pathLengthVector, vectorPos);
        vectorPos++;
        multiplicity--;
    }
}

void FrontiersScanner::scan(RecursiveJoinVectors& vectors, sel_t& vectorPos,
    sel_t& nodeIDDataVectorPos, sel_t& relIDDataVectorPos) {
    while (vectorPos < DEFAULT_VECTOR_CAPACITY && cursor < scanners.size()) {
        if (scanners[cursor]->scan(vectors, vectorPos, nodeIDDataVectorPos, relIDDataVectorPos) ==
            0) {
            cursor++;
        }
    }
}

} // namespace processor
} // namespace kuzu
