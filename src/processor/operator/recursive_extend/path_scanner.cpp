#include "processor/operator/recursive_extend/path_scanner.h"

namespace kuzu {
namespace processor {

size_t BaseFrontierScanner::scan(common::table_id_t tableID, common::ValueVector* pathVector,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) {
    if (k >= frontiers.size()) {
        // BFS terminate before current depth. No need to scan.
        return 0;
    }
    auto offsetVectorPosBeforeScanning = offsetVectorPos;
    auto lastFrontier = frontiers[k];
    while (true) {
        if (currentDstOffset != common::INVALID_OFFSET) {
            scanFromDstOffset(tableID, pathVector, dstNodeIDVector, pathLengthVector,
                offsetVectorPos, dataVectorPos);
        }
        if (offsetVectorPos == common::DEFAULT_VECTOR_CAPACITY) {
            break;
        }
        if (lastFrontierCursor == lastFrontier->nodeOffsets.size()) {
            // All nodes from last frontier have been scanned.
            currentDstOffset = common::INVALID_OFFSET;
            break;
        }
        currentDstOffset = lastFrontier->nodeOffsets[lastFrontierCursor++];
        // Skip nodes that is not in semi mask.
        if (!targetDstOffsets.empty() && !targetDstOffsets.contains(currentDstOffset)) {
            continue;
        }
        initScanFromDstOffset();
    }
    return offsetVectorPos - offsetVectorPosBeforeScanning;
}

void BaseFrontierScanner::resetState(const BaseBFSMorsel& bfsMorsel) {
    lastFrontierCursor = 0;
    currentDstOffset = common::INVALID_OFFSET;
    frontiers.clear();
    for (auto& frontier : bfsMorsel.frontiers) {
        frontiers.push_back(frontier.get());
    }
}

void PathScanner::scanFromDstOffset(common::table_id_t tableID, common::ValueVector* pathVector,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) {
    auto level = 0;
    while (!nbrsStack.empty()) {
        auto& cursor = cursorStack.top();
        cursor++;
        if (cursor < nbrsStack.top()->size()) { // Found a new nbr
            auto offset = nbrsStack.top()->at(cursor);
            path[level] = offset;
            if (level == 0) { // Found a new nbr at level 0. Found a new path.
                writePathToVector(tableID, pathVector, dstNodeIDVector, pathLengthVector,
                    offsetVectorPos, dataVectorPos);
                if (offsetVectorPos == common::DEFAULT_VECTOR_CAPACITY) {
                    return;
                }
                continue;
            }
            // Push new stack.
            cursorStack.push(-1);
            nbrsStack.push(&frontiers[level]->bwdEdges.at(offset));
            level--;
        } else { // Failed to find a nbr. Pop stack.
            cursorStack.pop();
            nbrsStack.pop();
            level++;
        }
    }
}

void PathScanner::initDfs(common::offset_t offset, size_t currentDepth) {
    path[currentDepth] = offset;
    if (currentDepth == 0) {
        cursorStack.top() = -1;
        return;
    }
    auto nbrs = &frontiers[currentDepth]->bwdEdges.at(offset);
    nbrsStack.push(nbrs);
    cursorStack.push(0);
    initDfs(nbrs->at(0), currentDepth - 1);
}

void PathScanner::writePathToVector(common::table_id_t tableID, common::ValueVector* pathVector,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) {
    assert(offsetVectorPos < common::DEFAULT_VECTOR_CAPACITY);
    auto pathLength = path.size();
    auto listEntry = common::ListVector::addList(pathVector, pathLength);
    pathVector->setValue(offsetVectorPos, listEntry);
    assert(path[pathLength - 1] == currentDstOffset);
    writeDstNodeOffsetAndLength(dstNodeIDVector, pathLengthVector, offsetVectorPos, tableID);
    offsetVectorPos++;
    auto pathDataVector = common::ListVector::getDataVector(pathVector);
    for (auto i = 0u; i < pathLength; ++i) {
        pathDataVector->setValue<common::nodeID_t>(
            dataVectorPos++, common::nodeID_t{path[i], tableID});
    }
}

void DstNodeWithMultiplicityScanner::scanFromDstOffset(common::table_id_t tableID,
    common::ValueVector* pathVector, common::ValueVector* dstNodeIDVector,
    common::ValueVector* pathLengthVector, common::sel_t& offsetVectorPos,
    common::sel_t& dataVectorPos) {
    auto& multiplicity = frontiers[k]->offsetToMultiplicity.at(currentDstOffset);
    while (multiplicity > 0 && offsetVectorPos < common::DEFAULT_VECTOR_CAPACITY) {
        writeDstNodeOffsetAndLength(dstNodeIDVector, pathLengthVector, offsetVectorPos, tableID);
        offsetVectorPos++;
        multiplicity--;
    }
}

void FrontiersScanner::scan(common::table_id_t tableID, common::ValueVector* pathVector,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) {
    while (offsetVectorPos < common::DEFAULT_VECTOR_CAPACITY && cursor < scanners.size()) {
        if (scanners[cursor]->scan(tableID, pathVector, dstNodeIDVector, pathLengthVector,
                offsetVectorPos, dataVectorPos) == 0) {
            cursor++;
        }
    }
}

} // namespace processor
} // namespace kuzu
