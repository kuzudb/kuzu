#include "processor/operator/recursive_extend/frontier_scanner.h"

namespace kuzu {
namespace processor {

size_t BaseFrontierScanner::scan(common::ValueVector* pathVector,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) {
    if (k >= frontiers.size()) {
        // BFS terminate before current depth. No need to scan.
        return 0;
    }
    auto offsetVectorPosBeforeScanning = offsetVectorPos;
    auto lastFrontier = frontiers[k];
    while (true) {
        if (currentDstNodeID.offset != common::INVALID_OFFSET) {
            scanFromDstOffset(
                pathVector, dstNodeIDVector, pathLengthVector, offsetVectorPos, dataVectorPos);
        }
        if (offsetVectorPos == common::DEFAULT_VECTOR_CAPACITY) {
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
    return offsetVectorPos - offsetVectorPosBeforeScanning;
}

void BaseFrontierScanner::resetState(const BaseBFSState& bfsState) {
    lastFrontierCursor = 0;
    currentDstNodeID = {common::INVALID_OFFSET, common::INVALID_TABLE_ID};
    frontiers.clear();
    for (auto i = 0; i < bfsState.getNumFrontiers(); ++i) {
        frontiers.push_back(bfsState.getFrontier(i));
    }
}

void PathScanner::scanFromDstOffset(common::ValueVector* pathVector,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) {
    auto level = 0;
    while (!nbrsStack.empty()) {
        auto& cursor = cursorStack.top();
        cursor++;
        if (cursor < nbrsStack.top()->size()) { // Found a new nbr
            auto& nbr = nbrsStack.top()->at(cursor);
            nodeIDs[level] = nbr.first;
            relIDs[level] = nbr.second;
            if (level == 0) { // Found a new nbr at level 0. Found a new path.
                writePathToVector(
                    pathVector, dstNodeIDVector, pathLengthVector, offsetVectorPos, dataVectorPos);
                if (offsetVectorPos == common::DEFAULT_VECTOR_CAPACITY) {
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

void PathScanner::writePathToVector(common::ValueVector* pathVector,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) {
    assert(offsetVectorPos < common::DEFAULT_VECTOR_CAPACITY);
    auto listEntry = common::ListVector::addList(pathVector, listEntrySize);
    pathVector->setValue(offsetVectorPos, listEntry);
    writeDstNodeOffsetAndLength(dstNodeIDVector, pathLengthVector, offsetVectorPos);
    offsetVectorPos++;
    auto pathDataVector = common::ListVector::getDataVector(pathVector);
    for (auto i = 0u; i < k; ++i) {
        pathDataVector->setValue<common::nodeID_t>(dataVectorPos++, nodeIDs[i]);
        pathDataVector->setValue<common::relID_t>(dataVectorPos++, relIDs[i]);
    }
    pathDataVector->setValue<common::nodeID_t>(dataVectorPos++, nodeIDs[k]);
}

void DstNodeWithMultiplicityScanner::scanFromDstOffset(common::ValueVector* pathVector,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) {
    auto& multiplicity = frontiers[k]->nodeIDToMultiplicity.at(currentDstNodeID);
    while (multiplicity > 0 && offsetVectorPos < common::DEFAULT_VECTOR_CAPACITY) {
        writeDstNodeOffsetAndLength(dstNodeIDVector, pathLengthVector, offsetVectorPos);
        offsetVectorPos++;
        multiplicity--;
    }
}

void FrontiersScanner::scan(common::ValueVector* pathVector, common::ValueVector* dstNodeIDVector,
    common::ValueVector* pathLengthVector, common::sel_t& offsetVectorPos,
    common::sel_t& dataVectorPos) {
    while (offsetVectorPos < common::DEFAULT_VECTOR_CAPACITY && cursor < scanners.size()) {
        if (scanners[cursor]->scan(pathVector, dstNodeIDVector, pathLengthVector, offsetVectorPos,
                dataVectorPos) == 0) {
            cursor++;
        }
    }
}

} // namespace processor
} // namespace kuzu
