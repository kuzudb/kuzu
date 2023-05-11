#include "processor/operator/recursive_extend/path_scanner.h"

namespace kuzu {
namespace processor {

size_t FixedLengthPathScanner::scanPaths(common::table_id_t tableID,
    common::ValueVector* pathOffsetVector, common::ValueVector* pathDataVector,
    common::ValueVector* dstNodeIDVector, common::sel_t& offsetVectorPos,
    common::sel_t& dataVectorPos) {
    if (numRels >= bfsMorsel.frontiers.size()) {
        // BFS terminate before current depth. No need to scan.
        return 0;
    }
    auto offsetVectorPosBeforeScanning = offsetVectorPos;
    auto lastFrontier = bfsMorsel.frontiers[numRels].get();
    while (true) {
        continueDfs(tableID, pathOffsetVector, pathDataVector, dstNodeIDVector, offsetVectorPos,
            dataVectorPos);
        if (offsetVectorPos == common::DEFAULT_VECTOR_CAPACITY) {
            break;
        }
        if (lastFrontierCursor == lastFrontier->nodeOffsets.size()) {
            // All nodes from last frontier have been scanned.
            break;
        }
        auto currentDstOffset = lastFrontier->nodeOffsets[lastFrontierCursor++];
        // Skip nodes that is not in semi mask.
        if (!bfsMorsel.isAllDstTarget() &&
            !bfsMorsel.targetDstNodeOffsets.contains(currentDstOffset)) {
            continue;
        }
        // Prepare dfs stack.
        initDfs(currentDstOffset, numRels);
    }
    return offsetVectorPos - offsetVectorPosBeforeScanning;
}

void FixedLengthPathScanner::continueDfs(common::table_id_t tableID,
    common::ValueVector* pathOffsetVector, common::ValueVector* pathDataVector,
    common::ValueVector* dstNodeIDVector, common::sel_t& offsetVectorPos,
    common::sel_t& dataVectorPos) {
    auto level = 0;
    while (!nbrsStack.empty()) {
        auto& cursor = cursorStack.top();
        cursor++;
        if (cursor < nbrsStack.top()->size()) { // Found a new nbr
            auto offset = nbrsStack.top()->at(cursor);
            path[level] = offset;
            if (level == 0) { // Found a new nbr at level 0. Found a new path.
                writePathToVector(tableID, pathOffsetVector, pathDataVector, dstNodeIDVector,
                    offsetVectorPos, dataVectorPos);
                if (offsetVectorPos == common::DEFAULT_VECTOR_CAPACITY) {
                    return;
                }
                continue;
            }
            // Push new stack.
            cursorStack.push(-1);
            nbrsStack.push(&bfsMorsel.frontiers[level]->bwdEdges.at(offset));
            level--;
        } else { // Failed to find a nbr. Pop stack.
            cursorStack.pop();
            nbrsStack.pop();
            level++;
        }
    }
}

void FixedLengthPathScanner::initDfs(common::offset_t offset, size_t currentDepth) {
    path[currentDepth] = offset;
    if (currentDepth == 0) {
        cursorStack.top() = -1;
        return;
    }
    auto nbrs = &bfsMorsel.frontiers[currentDepth]->bwdEdges.at(offset);
    nbrsStack.push(nbrs);
    cursorStack.push(0);
    initDfs(nbrs->at(0), currentDepth - 1);
}

void FixedLengthPathScanner::writePathToVector(common::table_id_t tableID,
    common::ValueVector* pathOffsetVector, common::ValueVector* pathDataVector,
    common::ValueVector* dstNodeIDVector, common::sel_t& offsetVectorPos,
    common::sel_t& dataVectorPos) {
    auto pathLength = path.size();
    auto listEntry = common::ListVector::addList(pathOffsetVector, pathLength);
    pathOffsetVector->setValue(offsetVectorPos, listEntry);
    dstNodeIDVector->setValue<common::nodeID_t>(
        offsetVectorPos, common::nodeID_t{path[pathLength - 1], tableID});
    assert(offsetVectorPos < common::DEFAULT_VECTOR_CAPACITY);
    offsetVectorPos++;
    for (auto i = 0u; i < pathLength; ++i) {
        pathDataVector->setValue<common::nodeID_t>(
            dataVectorPos++, common::nodeID_t{path[i], tableID});
    }
}

void PathScanner::scanPaths(common::table_id_t tableID, common::ValueVector* pathOffsetVector,
    common::ValueVector* pathDataVector, common::ValueVector* dstNodeIDVector,
    common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) {
    while (offsetVectorPos < common::DEFAULT_VECTOR_CAPACITY && cursor < scanners.size()) {
        if (scanners[cursor]->scanPaths(tableID, pathOffsetVector, pathDataVector, dstNodeIDVector,
                offsetVectorPos, dataVectorPos) == 0) {
            cursor++;
        }
    }
}

} // namespace processor
} // namespace kuzu
