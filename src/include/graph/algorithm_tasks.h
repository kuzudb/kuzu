#include <vector>

#include "common/types/internal_id_t.h"

namespace kuzu {
namespace graph {

enum Algorithm_Task_Type {
    BELLMAN_FORD,
    PAGE_RANK,
    UNWEIGHTED_SHORTEST_PATH,
};

struct AlgorithmTask {
public:
    explicit AlgorithmTask(Algorithm_Task_Type algorithmTaskType) : taskType{algorithmTaskType} {}

    virtual ~AlgorithmTask() = default;

    inline Algorithm_Task_Type getTaskType() {
        return taskType;
    }

    virtual uint64_t measureCurrentWork();

private:
    Algorithm_Task_Type taskType;
};

struct BellmanFordTask : public AlgorithmTask {
public:
    BellmanFordTask(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : AlgorithmTask(BELLMAN_FORD), mutex{std::mutex()}, currentLevel{0u}, nextScanStartIdx{0u},
          numVisitedNodes{0u}, pathLength{std::vector<uint8_t>(maxNodeOffset_ + 1, 0u)},
          bfsLevelNodeOffsets{std::vector<common::offset_t>()}, srcOffset{0u},
          maxOffset{maxNodeOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_},
          numThreadsBFSActive{0u}, nextDstScanStartIdx{0u}, inputFTableTupleIdx{0u} {}

    uint64_t measureCurrentWork() override {
        return (bfsLevelNodeOffsets.size() - nextScanStartIdx) / numThreadsBFSActive;
    }

public:
    std::mutex mutex;
    Algorithm_Task_Type taskType;
    uint8_t currentLevel;
    uint64_t nextScanStartIdx;

    // Visited state
    uint64_t numVisitedNodes;
    std::vector<uint8_t> visitedNodes;
    std::vector<uint8_t> pathLength;
    std::vector<common::offset_t> bfsLevelNodeOffsets;
    // Offset of src node.
    common::offset_t srcOffset;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    uint32_t numThreadsBFSActive;
    uint64_t nextDstScanStartIdx;
    uint64_t inputFTableTupleIdx;

    // FOR DOING `Weighted Shortest Path` (Returning path cost + the least cost path)
    std::vector<int64_t> pathCost;
    std::vector<int64_t> offsetPrevPathCost;
    std::vector<unsigned __int128> pathCostAndSrc;
    std::vector<std::pair<common::offset_t, common::offset_t>*> intermediateSrcAndEdges;
};

struct PageRankTask : public AlgorithmTask {

};

}
}
