#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

enum class VisitedState : uint8_t {
    NOT_VISITED = 0,
    VISITED = 1,
};

struct BFSLevelMorsel {
    uint64_t startIdx;
    uint64_t size;

    inline bool empty() const { return size == 0; }
};

struct BFSLevel {
    std::vector<common::offset_t> nodeOffsets;

    inline void resetState() { nodeOffsets.clear(); }
    inline uint64_t size() const { return nodeOffsets.size(); }
};

struct BFSMorsel {
    std::mutex mtx;

    // current/next level information
    uint8_t upperBound;
    uint8_t currentLevel;
    uint64_t currentLevelStartIdx;
    std::unique_ptr<BFSLevel> currentBFSLevel;
    std::unique_ptr<BFSLevel> nextBFSLevel;

    // visited nodes information
    common::offset_t maxOffset;
    uint8_t* visitedNodes;
    std::unique_ptr<uint8_t[]> visitedNodesBuffer;

    BFSMorsel(common::offset_t maxOffset_);

    // Reset state to start BFS again for a new src node.
    void resetState();
    bool isComplete();

    // Get range to scan from current level.
    std::unique_ptr<BFSLevelMorsel> getBFSLevelMorsel();

    void moveNextLevelAsCurrentLevel();
};


class BFSThreadLocalSharedState {

};

class ScanBFSLevel : public PhysicalOperator {
public:
    ScanBFSLevel(storage::NodeTable* nodeTable, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_BFS_LEVEL, std::move(child), id,
              paramsString},
          nodeTable{nodeTable} {}

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    void writeBFSLevelMorselToTmpSrcNodeIDsVector(
        BFSLevelMorsel* bfsLevelMorsel, BFSLevel* bfsLevel);

private:
    storage::NodeTable* nodeTable;
    uint8_t upperBound;

    std::unique_ptr<BFSMorsel> bfsMorsel;
    std::shared_ptr<common::ValueVector> srcNodeIDs;

    // Temporary vectors used for recursive joins
    common::ValueVector* tmpSrcNodeIDs;
    common::ValueVector* tmpDstNodeIDs;
};

} // namespace processor
} // namespace kuzu