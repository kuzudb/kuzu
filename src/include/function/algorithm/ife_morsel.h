#include <vector>

namespace kuzu {
namespace graph {

enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
    VISITED_NEW = 4,
    VISITED_DST_NEW = 5,
};

struct IFEMorsel {
public:
    IFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : mutex{std::mutex()}, currentLevel{0u},
          nextScanStartIdx{0u}, numVisitedNodes{0u}, visitedNodes{std::vector<uint8_t>(
                                                         maxNodeOffset_ + 1, NOT_VISITED)},
          pathLength{std::vector<uint8_t>(maxNodeOffset_ + 1, 0u)},
          bfsLevelNodeOffsets{std::vector<common::offset_t>()}, srcOffset{0u},
          maxOffset{maxNodeOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_},
          numThreadsBFSActive{0u}, nextDstScanStartIdx{0u} {}



    /*void reset(TargetDstNodes* targetDstNodes, common::QueryRelType queryRelType,
        planner::RecursiveJoinType joinType);

    SSSPLocalState getBFSMorsel(BaseBFSMorsel* bfsMorsel);

    bool finishBFSMorsel(BaseBFSMorsel* bfsMorsel, common::QueryRelType queryRelType);

    // If BFS has completed.
    bool isBFSComplete(uint64_t numDstNodesToVisit, common::QueryRelType queryRelType);
    // Mark src as visited.
    void markSrc(bool isSrcDestination, common::QueryRelType queryRelType);

    void moveNextLevelAsCurrentLevel();

    std::pair<uint64_t, int64_t> getDstPathLengthMorsel();*/

public:
    std::mutex mutex;
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
};

}
}
