#include "processor/operator/recursive_extend/frontier_scanner.h"

#include "processor/operator/recursive_extend/recursive_join.h"

namespace kuzu {
namespace processor {

size_t BaseFrontierScanner::scan(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
    common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos) {
    if (k >= frontiers.size()) {
        // BFS terminate before current depth. No need to scan.
        return 0;
    }
    auto vectorPosBeforeScanning = vectorPos;
    auto lastFrontier = frontiers[k];
    while (true) {
        if (currentDstNodeID.offset != common::INVALID_OFFSET) {
            scanFromDstOffset(vectors, vectorPos, nodeIDDataVectorPos, relIDDataVectorPos);
        }
        if (vectorPos == common::DEFAULT_VECTOR_CAPACITY) {
            break;
        }
        if (lastFrontierCursor == lastFrontier->nodeIDs.size()) {
            // All nodes from last frontier have been scanned.
            currentDstNodeID = {common::INVALID_OFFSET, common::INVALID_TABLE_ID};
            break;
        }
        currentDstNodeID = lastFrontier->nodeIDs[lastFrontierCursor++];
        // Skip nodes that is not in semi mask.
        if (!targetDstNodes->contains(currentDstNodeID)) {
            currentDstNodeID.offset = common::INVALID_OFFSET;
            continue;
        }
        initScanFromDstOffset();
    }
    return vectorPos - vectorPosBeforeScanning;
}

void BaseFrontierScanner::resetState(const BaseBFSState& bfsState) {
    lastFrontierCursor = 0;
    currentDstNodeID = {common::INVALID_OFFSET, common::INVALID_TABLE_ID};
    frontiers.clear();
    for (auto i = 0; i < bfsState.getNumFrontiers(); ++i) {
        frontiers.push_back(bfsState.getFrontier(i));
    }
}

void DstNodeScanner::scanFromDstOffset(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
    common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos) {
    assert(vectorPos < common::DEFAULT_VECTOR_CAPACITY);
    writeDstNodeOffsetAndLength(vectors->dstNodeIDVector, vectors->pathLengthVector, vectorPos);
    vectorPos++;
}

void PathScanner::scanFromDstOffset(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
    common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos) {
    auto level = 0;
    while (!nbrsStack.empty()) {
        auto& cursor = cursorStack.top();
        cursor++;
        if (cursor < nbrsStack.top()->size()) { // Found a new nbr
            auto& nbr = nbrsStack.top()->at(cursor);
            nodeIDs[level] = nbr.first;
            relIDs[level] = nbr.second;
            if (level == 0) { // Found a new nbr at level 0. Found a new path.
                writePathToVector(vectors, vectorPos, nodeIDDataVectorPos, relIDDataVectorPos);
                if (vectorPos == common::DEFAULT_VECTOR_CAPACITY) {
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

void PathScanner::initDfs(const frontier::node_rel_id_t& nodeAndRelID, size_t currentDepth) {
    nodeIDs[currentDepth] = nodeAndRelID.first;
    relIDs[currentDepth] = nodeAndRelID.second;
    if (currentDepth == 0) {
        cursorStack.top() = -1;
        return;
    }
    auto nbrs = &frontiers[currentDepth]->bwdEdges.at(nodeAndRelID.first);
    nbrsStack.push(nbrs);
    cursorStack.push(0);
    initDfs(nbrs->at(0), currentDepth - 1);
}

void PathScanner::writePathToVector(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
    common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos) {
    assert(vectorPos < common::DEFAULT_VECTOR_CAPACITY);
    auto nodeEntry = common::ListVector::addList(vectors->pathNodesVector, k + 1);
    auto relEntry = common::ListVector::addList(vectors->pathRelsVector, k);
    vectors->pathNodesVector->setValue(vectorPos, nodeEntry);
    vectors->pathRelsVector->setValue(vectorPos, relEntry);
    writeDstNodeOffsetAndLength(vectors->dstNodeIDVector, vectors->pathLengthVector, vectorPos);
    vectorPos++;
    for (auto i = 0u; i < k; ++i) {
        vectors->pathNodesIDDataVector->setValue<common::nodeID_t>(
            nodeIDDataVectorPos++, nodeIDs[i]);
        vectors->pathRelsIDDataVector->setValue<common::relID_t>(relIDDataVectorPos++, relIDs[i]);
    }
    vectors->pathNodesIDDataVector->setValue<common::nodeID_t>(nodeIDDataVectorPos++, nodeIDs[k]);
}

void DstNodeWithMultiplicityScanner::scanFromDstOffset(RecursiveJoinVectors* vectors,
    common::sel_t& vectorPos, common::sel_t& nodeIDDataVectorPos,
    common::sel_t& relIDDataVectorPos) {
    auto& multiplicity = frontiers[k]->nodeIDToMultiplicity.at(currentDstNodeID);
    while (multiplicity > 0 && vectorPos < common::DEFAULT_VECTOR_CAPACITY) {
        writeDstNodeOffsetAndLength(vectors->dstNodeIDVector, vectors->pathLengthVector, vectorPos);
        vectorPos++;
        multiplicity--;
    }
}

void FrontiersScanner::scan(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
    common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos) {
    while (vectorPos < common::DEFAULT_VECTOR_CAPACITY && cursor < scanners.size()) {
        if (scanners[cursor]->scan(vectors, vectorPos, nodeIDDataVectorPos, relIDDataVectorPos) ==
            0) {
            cursor++;
        }
    }
}

} // namespace processor
} // namespace kuzu
