#pragma once

#include <stack>

#include "bfs_state.h"

namespace kuzu {
namespace processor {

/*
 * BaseFrontierScanner scans all dst nodes from k'th frontier. To identify the
 * destination nodes in the k'th frontier, we use a semi mask that marks the destination nodes (or
 * targetDstOffsets.empty() which indicates that every node is a possible destination).
 */
struct BaseFrontierScanner {
    std::vector<Frontier*> frontiers;
    const std::unordered_set<common::offset_t>& targetDstOffsets;
    // Number of extension performed during recursive join
    size_t k;

    size_t lastFrontierCursor;
    common::offset_t currentDstOffset;

    BaseFrontierScanner(const std::unordered_set<common::offset_t>& targetDstOffsets, size_t k)
        : targetDstOffsets{targetDstOffsets}, k{k}, lastFrontierCursor{0},
          currentDstOffset{common::INVALID_OFFSET} {}
    virtual ~BaseFrontierScanner() = default;

    size_t scan(common::table_id_t tableID, common::ValueVector* pathVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos);

    void resetState(const BaseBFSMorsel& bfsMorsel);

protected:
    virtual void initScanFromDstOffset() = 0;
    virtual void scanFromDstOffset(common::table_id_t tableID, common::ValueVector* pathVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) = 0;

    inline void writeDstNodeOffsetAndLength(common::ValueVector* dstNodeIDVector,
        common::ValueVector* pathLengthVector, common::sel_t& offsetVectorPos,
        common::table_id_t tableID) {
        dstNodeIDVector->setValue<common::nodeID_t>(
            offsetVectorPos, common::nodeID_t{currentDstOffset, tableID});
        pathLengthVector->setValue<int64_t>(offsetVectorPos, (int64_t)k);
    }
};

/*
 * DstNodeScanner scans dst node offset & length of path.
 */
struct DstNodeScanner : public BaseFrontierScanner {
    DstNodeScanner(const std::unordered_set<common::offset_t>& targetDstOffsets, size_t k)
        : BaseFrontierScanner{targetDstOffsets, k} {}

private:
    inline void initScanFromDstOffset() final {}
    inline void scanFromDstOffset(common::table_id_t tableID, common::ValueVector* pathVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) final {
        assert(offsetVectorPos < common::DEFAULT_VECTOR_CAPACITY);
        writeDstNodeOffsetAndLength(dstNodeIDVector, pathLengthVector, offsetVectorPos, tableID);
        offsetVectorPos++;
    }
};

/*
 * PathScanner scans all paths of a fixed length k (also dst node offsets & length of path). This is
 * done by starting a backward traversals from only the destination nodes in the k'th frontier
 * (assuming the first frontier has index 0) over the backwards edges stored between the frontiers
 * that was used to store the data related to the BFS that was computed in the RecursiveJoin
 * operator.
 */
struct PathScanner : public BaseFrontierScanner {
    using nbrs_t = std::vector<common::offset_t>*;
    // DFS states
    std::vector<common::offset_t> path;
    std::stack<nbrs_t> nbrsStack;
    std::stack<int64_t> cursorStack;

    PathScanner(const std::unordered_set<common::offset_t>& targetDstOffsets, size_t k)
        : BaseFrontierScanner{targetDstOffsets, k} {
        path.resize(k + 1);
    }

private:
    inline void initScanFromDstOffset() final { initDfs(currentDstOffset, k); }
    // Scan current stacks until exhausted or vector is filled up.
    void scanFromDstOffset(common::table_id_t tableID, common::ValueVector* pathVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) final;

    // Initialize stacks for given offset.
    void initDfs(common::offset_t offset, size_t currentDepth);

    void writePathToVector(common::table_id_t tableID, common::ValueVector* pathVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos);
};

/*
 * DstNodeWithMultiplicityScanner scans dst node offset & length of path and repeat it for
 * multiplicity times in value vector.
 */
struct DstNodeWithMultiplicityScanner : public BaseFrontierScanner {
    DstNodeWithMultiplicityScanner(
        const std::unordered_set<common::offset_t>& targetDstOffsets, size_t k)
        : BaseFrontierScanner{targetDstOffsets, k} {}

private:
    inline void initScanFromDstOffset() final {}
    void scanFromDstOffset(common::table_id_t tableID, common::ValueVector* pathVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos) final;
};

/*
 * Variable-length joins return union of paths with different length (e.g. *2..3). Note that we only
 * keep track of the backward edges (if edges are tracked) between the frontier in the RecursiveJoin
 * operator (these frontiers are stored in the BaseBFSMorsel that was used to keep the data related
 * to the BFS that was computed in the RecursiveJoin). Therefore, we cannot start from the src and
 * traverse to find all paths of all lengths. We can only start from nodes in a particular frontier
 * and traverse backwards to the source. But whenever we start from a particular frontier, say the
 * k'th frontier, we can only traverse paths of length k. Therefore, PathScanner scans these paths
 * length by length, i.e. we first scan all length-2 paths, then scan all length-3 paths.
 */
struct FrontiersScanner {
    std::vector<std::unique_ptr<BaseFrontierScanner>> scanners;
    common::vector_idx_t cursor;

    explicit FrontiersScanner(std::vector<std::unique_ptr<BaseFrontierScanner>> scanners)
        : scanners{std::move(scanners)}, cursor{0} {}

    void scan(common::table_id_t tableID, common::ValueVector* pathVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::sel_t& offsetVectorPos, common::sel_t& dataVectorPos);

    inline void resetState(const BaseBFSMorsel& bfsMorsel) {
        cursor = 0;
        for (auto& scanner : scanners) {
            scanner->resetState(bfsMorsel);
        }
    }
};

} // namespace processor
} // namespace kuzu
