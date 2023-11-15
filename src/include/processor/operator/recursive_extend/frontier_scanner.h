#pragma once

#include <stack>

#include "bfs_state.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace processor {

struct RecursiveJoinVectors;
/*
 * BaseFrontierScanner scans all dst nodes from k'th frontier. To identify the
 * destination nodes in the k'th frontier, we use a semi mask that marks the destination nodes (or
 * targetDstOffsets.empty() which indicates that every node is a possible destination).
 */
class BaseFrontierScanner {
public:
    BaseFrontierScanner(TargetDstNodes* targetDstNodes, size_t k)
        : targetDstNodes{targetDstNodes}, k{k}, lastFrontierCursor{0},
          currentDstNodeID{common::INVALID_OFFSET, common::INVALID_TABLE_ID} {}
    virtual ~BaseFrontierScanner() = default;

    size_t scan(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
        common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos);

    void resetState(const BaseBFSState& bfsState);

protected:
    virtual void initScanFromDstOffset() = 0;
    virtual void scanFromDstOffset(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
        common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos) = 0;

    inline void writeDstNodeOffsetAndLength(common::ValueVector* dstNodeIDVector,
        common::ValueVector* pathLengthVector, common::sel_t& vectorPos) {
        dstNodeIDVector->setValue<common::nodeID_t>(vectorPos, currentDstNodeID);
        pathLengthVector->setValue<int64_t>(vectorPos, (int64_t)k);
    }

protected:
    std::vector<Frontier*> frontiers;
    TargetDstNodes* targetDstNodes;
    // Number of extension performed during recursive join
    size_t k;

    size_t lastFrontierCursor;
    common::nodeID_t currentDstNodeID;
};

/*
 * PathScanner scans all paths of a fixed length k (also dst node offsets & length of path). This is
 * done by starting a backward traversals from only the destination nodes in the k'th frontier
 * (assuming the first frontier has index 0) over the backwards edges stored between the frontiers
 * that was used to store the data related to the BFS that was computed in the RecursiveJoin
 * operator.
 */
class PathScanner : public BaseFrontierScanner {
    using nbrs_t = std::vector<frontier::node_rel_id_t>*;

public:
    PathScanner(TargetDstNodes* targetDstNodes, size_t k,
        std::unordered_map<common::table_id_t, std::string> tableIDToName)
        : BaseFrontierScanner{targetDstNodes, k}, tableIDToName{std::move(tableIDToName)} {
        nodeIDs.resize(k + 1);
        relIDs.resize(k + 1);
    }

private:
    inline void initScanFromDstOffset() final {
        auto dummyRelID = common::relID_t{common::INVALID_OFFSET, common::INVALID_TABLE_ID};
        initDfs(std::make_pair(currentDstNodeID, dummyRelID), k);
    }
    // Scan current stacks until exhausted or vector is filled up.
    void scanFromDstOffset(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
        common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos) final;

    // Initialize stacks for given offset.
    void initDfs(const frontier::node_rel_id_t& nodeAndRelID, size_t currentDepth);

    void writePathToVector(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
        common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos);

private:
    // DFS states
    std::vector<common::nodeID_t> nodeIDs;
    std::vector<common::relID_t> relIDs;
    std::stack<nbrs_t> nbrsStack;
    std::stack<int64_t> cursorStack;
    std::unordered_map<common::table_id_t, std::string> tableIDToName;
};

/*
 * DstNodeWithMultiplicityScanner scans dst node offset & length of path and repeat it for
 * multiplicity times in value vector.
 */
class DstNodeWithMultiplicityScanner : public BaseFrontierScanner {
public:
    DstNodeWithMultiplicityScanner(TargetDstNodes* targetDstNodes, size_t k)
        : BaseFrontierScanner{targetDstNodes, k} {}

private:
    inline void initScanFromDstOffset() final {}
    void scanFromDstOffset(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
        common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos) final;
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

    void scan(RecursiveJoinVectors* vectors, common::sel_t& vectorPos,
        common::sel_t& nodeIDDataVectorPos, common::sel_t& relIDDataVectorPos);

    inline void resetState(const BaseBFSState& bfsState) {
        cursor = 0;
        for (auto& scanner : scanners) {
            scanner->resetState(bfsState);
        }
    }
};

} // namespace processor
} // namespace kuzu
