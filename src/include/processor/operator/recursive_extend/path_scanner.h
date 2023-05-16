#pragma once

#include <stack>

#include "bfs_state.h"

namespace kuzu {
namespace processor {

/*
 * FixedLengPathScanner scans all paths of a fixed length k. This is done by starting a backward
 * traversals from only the destination nodes in the k'th frontier (assuming the first frontier has
 * index 0) over the backwards edges stored between the frontiers stored in the BFSMorsel that
 * was used to store the data related to the BFS that was computed in the RecursiveJoin operator.
 * To identify the destination nodes in the k'th frontier, we use a semi mask that marks the
 * destination nodes (or the BFSMorsel::isAllDstTarget() function which indicates that every node is
 * a possible destination).
 */
struct FixedLengthPathScanner {
    using nbrs_t = std::vector<common::offset_t>*;
    // FixedLengthPathScanner scans from bfsMorsel frontiers and semi mask.
    const BaseBFSMorsel& bfsMorsel;
    // Number of extension performed during recursive join
    size_t numRels;
    std::vector<common::offset_t> path;

    // DFS states
    std::stack<nbrs_t> nbrsStack;
    std::stack<int64_t> cursorStack;

    size_t lastFrontierCursor;

    FixedLengthPathScanner(const BaseBFSMorsel& bfsMorsel, size_t numRels)
        : bfsMorsel{bfsMorsel}, numRels{numRels} {
        path.resize(numRels + 1);
        resetState();
    }

    size_t scanPaths(common::table_id_t tableID, common::ValueVector* pathOffsetVector,
        common::ValueVector* pathDataVector, common::ValueVector* dstNodeIDVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos);

    inline void resetState() { lastFrontierCursor = 0; }

private:
    // Scan current stacks until exhausted or vector is filled up.
    void continueDfs(common::table_id_t tableID, common::ValueVector* pathOffsetVector,
        common::ValueVector* pathDataVector, common::ValueVector* dstNodeIDVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos);

    // Initialize stacks for given offset.
    void initDfs(common::offset_t offset, size_t currentDepth);

    void writePathToVector(common::table_id_t tableID, common::ValueVector* pathOffsetVector,
        common::ValueVector* pathDataVector, common::ValueVector* dstNodeIDVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos);
};

/*
 * Variable-length joins return union of paths with different length (e.g. *2..3). Note that we only
 * keep track of the backward edges between the frontier in the RecursiveJoin operator (these
 * frontiers are stored in the BaseBFSMorsel that was used to keep the data related to the BFS that
 * was computed in the RecursiveJoin). Therefore we cannot start from the src and traverse to find
 * all paths of all lengths. We can only start from nodes in a particular frontier and traverse
 * backwards to the source. But whenever we start from a particular frontier, say the k'th frontier,
 * we can only traverse paths of length k. Therefore, PathScanner scans these paths length by
 * length, i.e. we first scan all length-2 paths, then scan all length-3 paths.
 */
struct PathScanner {
    size_t lowerBound;
    size_t upperBound;
    std::vector<std::unique_ptr<FixedLengthPathScanner>> scanners;
    common::vector_idx_t cursor;

    PathScanner(const BaseBFSMorsel& bfsMorsel, size_t lowerBound, size_t upperBound)
        : lowerBound{lowerBound}, upperBound{upperBound}, cursor{0} {
        for (auto i = lowerBound; i <= upperBound; ++i) {
            scanners.push_back(std::make_unique<FixedLengthPathScanner>(bfsMorsel, i));
        }
    }

    void scanPaths(common::table_id_t tableID, common::ValueVector* pathOffsetVector,
        common::ValueVector* pathDataVector, common::ValueVector* dstNodeIDVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos);

    inline void resetState() {
        cursor = 0;
        for (auto& scanner : scanners) {
            scanner->resetState();
        }
    }
};

} // namespace processor
} // namespace kuzu
